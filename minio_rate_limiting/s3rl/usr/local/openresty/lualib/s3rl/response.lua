-- =============================================================================
-- /usr/local/openresty/lualib/s3rl/response.lua
-- 回傳 S3 相容的錯誤 XML。AWS SDK / mc / boto3 看到 SlowDown 會自動指數退避重試。
-- =============================================================================

local _M = { _VERSION = "1.0.0" }

local fmt, gsub = string.format, string.gsub

local ESC = { ["&"] = "&amp;", ["<"] = "&lt;", [">"] = "&gt;",
              ['"'] = "&quot;", ["'"] = "&apos;" }

local function esc(s)
    if not s then return "" end
    return (gsub(s, "[&<>\"']", ESC))
end

local TPL = '<?xml version="1.0" encoding="UTF-8"?>\n'
         .. '<Error><Code>%s</Code><Message>%s</Message>'
         .. '<BucketName>%s</BucketName><Resource>%s</Resource>'
         .. '<RequestId>%s</RequestId><HostId>%s</HostId></Error>'

--- @param opt table {status, code, message, bucket, retry_after, request_id, headers}
function _M.error(opt)
    local status = opt.status or 503
    ngx.status = status

    ngx.header["Content-Type"]     = "application/xml"
    ngx.header["Retry-After"]      = tostring(opt.retry_after or 1)
    ngx.header["x-amz-request-id"] = opt.request_id
    ngx.header["x-amz-id-2"]       = opt.request_id
    ngx.header["Cache-Control"]    = "no-store"
    for k, v in pairs(opt.headers or {}) do ngx.header[k] = v end

    if ngx.req.get_method() == "HEAD" then
        ngx.header["Content-Length"] = 0
        return ngx.exit(status)
    end

    local body = fmt(TPL,
        opt.code or "SlowDown",
        esc(opt.message or "Please reduce your request rate."),
        esc(opt.bucket or ""),
        esc(ngx.var.request_uri or ""),
        opt.request_id or "-",
        opt.request_id or "-")

    ngx.header["Content-Length"] = #body
    ngx.print(body)
    return ngx.exit(status)
end

return _M
