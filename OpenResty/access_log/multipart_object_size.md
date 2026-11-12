根本原因是 S3 multipart 協定裡沒有任何一個 request 帶有完整的 object size：`UploadPart` 的 `Content-Length` 只是該 part 的大小，而 `CompleteMultipartUpload` 的 body 只有 PartNumber 和 ETag 的 XML。所以要在 gateway 以 `uploadId` 為 key 自己累加，在 Complete 成功時寫進 log。

## OpenResty 做法

做法是每個成功的 `UploadPart` 把 size 存進 `lua_shared_dict`，Complete 時只加總 body 裡列出的 parts。`log_by_lua` 會在 access log 寫出之前執行，所以在裡面設定 `ngx.var` 可以直接被 `log_format` 用到。以下程式碼我沒有實際跑過，請先在測試環境驗證。

```nginx
http {
    lua_shared_dict s3_mpu 256m;

    log_format s3_json escape=json
        '{"ts":"$time_iso8601","method":"$request_method","uri":"$uri",'
        '"status":$status,"s3_op":"$s3_op","upload_id":"$s3_upload_id",'
        '"part_size":"$s3_part_size","object_size":"$s3_object_size"}';

    server {
        access_log logs/s3_access.log s3_json;

        location / {
            set $s3_op "-";
            set $s3_upload_id "-";
            set $s3_part_size "-";
            set $s3_object_size "-";

            # 只有 CompleteMultipartUpload 需要讀 body，取出實際被 commit 的 parts
            access_by_lua_block {
                local args = ngx.req.get_uri_args()
                if ngx.req.get_method() == "POST" and args.uploadId then
                    ngx.req.read_body()
                    local body = ngx.req.get_body_data()
                    if not body then  -- body 超過 client_body_buffer_size 時會落到 temp file
                        local path = ngx.req.get_body_file()
                        local fh = path and io.open(path, "rb")
                        if fh then body = fh:read("*a"); fh:close() end
                    end
                    local parts = {}
                    for n in (body or ""):gmatch("<PartNumber>%s*(%d+)%s*</PartNumber>") do
                        parts[#parts + 1] = n
                    end
                    ngx.ctx.mpu_parts = parts
                end
            }

            proxy_pass http://s3_backend;

            log_by_lua_block {
                local dict   = ngx.shared.s3_mpu
                local method = ngx.req.get_method()
                local args   = ngx.req.get_uri_args()
                local hdr    = ngx.req.get_headers()
                local status = ngx.status
                local TTL    = 7 * 86400

                local function payload_size()
                    if hdr["x-amz-copy-source"] then  -- UploadPartCopy / CopyObject 沒有 body
                        local a, b = (hdr["x-amz-copy-source-range"] or ""):match("bytes=(%d+)-(%d+)")
                        return a and (tonumber(b) - tonumber(a) + 1) or nil
                    end
                    -- aws-chunked 時 Content-Length 含 chunk signature overhead
                    return tonumber(hdr["x-amz-decoded-content-length"])
                        or tonumber(hdr["content-length"])
                end

                local upload_id = args.uploadId
                if type(upload_id) ~= "string" then
                    -- 非 multipart；這裡的分類是簡化版，?tagging / ?acl 等 subresource 要另外排除
                    if method == "PUT" and status == 200 then
                        local size = payload_size()
                        if size then ngx.var.s3_object_size = string.format("%.0f", size) end
                    end
                    return
                end

                ngx.var.s3_upload_id = upload_id
                local prefix = ngx.md5(upload_id) .. ":"

                if method == "PUT" and args.partNumber then
                    ngx.var.s3_op = "UploadPart"
                    local size = payload_size()
                    if status == 200 and size then
                        ngx.var.s3_part_size = string.format("%.0f", size)
                        -- 用 uploadId:partNumber 當 key，同一個 part 重傳會覆蓋而不是重複累加
                        local ok, err, forcible = dict:set(prefix .. args.partNumber, size, TTL)
                        if not ok or forcible then
                            ngx.log(ngx.WARN, "s3_mpu dict pressure: ", err or "evicting")
                        end
                    end

                elseif method == "POST" then
                    ngx.var.s3_op = "CompleteMultipartUpload"
                    if status == 200 then
                        local total, missing = 0, 0
                        for _, n in ipairs(ngx.ctx.mpu_parts or {}) do
                            local k = prefix .. n
                            local size = dict:get(k)
                            if size then total = total + size else missing = missing + 1 end
                            dict:delete(k)
                        end
                        local s = string.format("%.0f", total)
                        ngx.var.s3_object_size = (missing == 0) and s or ("incomplete:" .. s)
                    end

                elseif method == "DELETE" then
                    ngx.var.s3_op = "AbortMultipartUpload"  -- 殘留的 parts 靠 TTL 過期
                end
            }
        }
    }
}
```

## 會影響精準度的地方

**Part 重傳與未 commit 的 parts。** Client retry 時同一個 partNumber 可能上傳多次，也可能上傳了卻沒列在 Complete 裡。所以上面用 per-part key 並只加總 Complete body 列出的 parts，而不是對 `uploadId` 做單純的 `incr`。

**aws-chunked encoding。** SigV4 streaming upload 的 `Content-Length` 包含每個 chunk 的 signature，真正的 payload size 在 `x-amz-decoded-content-length`。這對單一 `PutObject` 也成立，你現有的 log 可能已經偏大。

**多台 OpenResty。** `lua_shared_dict` 只在單一 instance 的 workers 之間共享，parts 被 LB 分散到不同節點時就加不起來。可以改用 Redis，但 `log_by_lua` 不能用 cosocket，寫入要包在 `ngx.timer.at(0, ...)` 裡，讀取要移到 Complete 的 access phase。

你的 gateway access events 本來就會進 Kafka 和 ClickHouse，所以我更建議讓 gateway 保持 stateless：只 log `upload_id`、`part_number`、`part_size`，Complete 時 log parts 清單，加總交給 ClickHouse 以 `upload_id` 聚合。

**Complete 回 200 但 body 是 Error。** S3 語意允許這種情況，只看 status 會誤記。要嚴謹的話，在 `body_filter_by_lua` 檢查 response 是否含 `<Error>`。

**State 遺失。** Reload 不會清掉 shared dict，但 restart 會，進行中的 upload 會被標成 `incomplete:`。

## Source of truth

Gateway 端算出來的終究是推導值。MinIO 的 bucket notification（`s3:ObjectCreated:CompleteMultipartUpload`）裡的 `s3.object.size` 才是權威數字，可以直接送 Kafka。建議 access log 的 `object_size` 當即時值，再用 MinIO event 以 bucket + key + versionId 在 ClickHouse 對帳或覆寫，`UploadPartCopy` 沒帶 range 這類 gateway 算不出來的情況也能補齊。
