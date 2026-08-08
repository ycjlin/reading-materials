-- =============================================================================
-- /usr/local/openresty/lualib/s3rl/parser.lua
-- 從 S3 request 解析出 bucket / key / operation class / access key
-- =============================================================================

local _M = { _VERSION = "1.0.0" }

local sub          = string.sub
local find         = string.find
local lower        = string.lower
local ngx_re_match = ngx.re.match

local vhost_suffixes = {}   -- { ".s3.fab.local", ".minio.fab.local" }

function _M.init(domains)
    vhost_suffixes = {}
    for i = 1, #(domains or {}) do
        vhost_suffixes[#vhost_suffixes + 1] = "." .. lower(domains[i])
    end
end

local function host_only(h)
    if not h or h == "" then return "" end
    local p = find(h, ":", 1, true)
    return lower(p and sub(h, 1, p - 1) or h)
end

---------------------------------------------------------------------------
-- bucket / key
-- 回傳 bucket(string|nil), key(string), style("vhost"|"path"|"service")
---------------------------------------------------------------------------
function _M.bucket_and_key(http_host, uri)
    local h = host_only(http_host)

    -- virtual-hosted-style: <bucket>.s3.fab.local/<key>
    for i = 1, #vhost_suffixes do
        local suf, sl = vhost_suffixes[i], #vhost_suffixes[i]
        if #h > sl and sub(h, -sl) == suf then
            local b = sub(h, 1, #h - sl)
            if b ~= "" and not find(b, ".", 1, true) then
                return b, sub(uri, 2), "vhost"
            end
        end
    end

    -- path-style: s3.fab.local/<bucket>/<key>
    if #uri < 2 then
        return nil, "", "service"          -- GET / => ListBuckets
    end
    local p = find(uri, "/", 2, true)
    if p then
        return sub(uri, 2, p - 1), sub(uri, p + 1), "path"
    end
    return sub(uri, 2), "", "path"
end

---------------------------------------------------------------------------
-- operation class
-- list  : ListObjectsV2 / ListObjectVersions / ListParts / SelectObjectContent
-- read  : GetObject
-- write : PutObject / UploadPart / CopyObject / CompleteMultipartUpload
-- delete: DeleteObject / DeleteObjects(batch) / AbortMultipartUpload
-- meta  : Head* / bucket 與 object 的 sub-resource
-- other : 其餘
---------------------------------------------------------------------------
local SUBRES_META = {
    acl = true, policy = true, tagging = true, lifecycle = true, cors = true,
    website = true, encryption = true, versioning = true, location = true,
    notification = true, replication = true, logging = true, accelerate = true,
    requestPayment = true, attributes = true, retention = true,
    ["object-lock"] = true, ["legal-hold"] = true, ["publicAccessBlock"] = true,
    ["ownershipControls"] = true, ["intelligent-tiering"] = true,
}

local function has_meta_subres(args)
    for k in pairs(args) do
        if SUBRES_META[k] then return true end
    end
    return false
end

function _M.classify(method, bucket, key, args)
    if not bucket then return "list" end          -- ListBuckets
    local has_key = key ~= nil and key ~= ""

    if method == "GET" then
        if args.uploadId ~= nil then return "list" end        -- ListParts
        if not has_key then
            if args.uploads  ~= nil then return "list" end    -- ListMultipartUploads
            if args.versions ~= nil then return "list" end    -- ListObjectVersions
            if has_meta_subres(args)  then return "meta" end
            return "list"                                    -- ListObjects / V2
        end
        if has_meta_subres(args) then return "meta" end
        return "read"                                        -- GetObject

    elseif method == "HEAD" then
        return "meta"                                        -- HeadObject / HeadBucket

    elseif method == "PUT" then
        if not has_key            then return "meta"  end    -- CreateBucket / bucket cfg
        if has_meta_subres(args)  then return "meta"  end
        return "write"                                       -- PutObject / UploadPart / Copy

    elseif method == "POST" then
        if args["delete"] ~= nil then return "delete" end    -- DeleteObjects (batch)
        if args.uploads   ~= nil then return "write"  end    -- CreateMultipartUpload
        if args.uploadId  ~= nil then return "write"  end    -- CompleteMultipartUpload
        if args.select    ~= nil then return "list"   end    -- SelectObjectContent（很貴）
        if args.restore   ~= nil then return "write"  end
        return "other"

    elseif method == "DELETE" then
        if not has_key then return "meta" end                -- DeleteBucket
        return "delete"                                      -- DeleteObject / Abort MPU
    end

    return "other"
end

---------------------------------------------------------------------------
-- access key（用於 per-tenant 維度 / 白名單）
--   SigV4 header : Authorization: AWS4-HMAC-SHA256 Credential=AK/2026.../s3/...
--   SigV4 presign: ?X-Amz-Credential=AK%2F2026...
--   SigV2 header : Authorization: AWS AK:signature
---------------------------------------------------------------------------
local RE_V4 = [[Credential=([^/,\s]+)/]]
local RE_V2 = [[^AWS\s+([^:\s]+):]]

function _M.access_key(auth, args)
    if auth and auth ~= "" then
        local m = ngx_re_match(auth, RE_V4, "jo")
        if m then return m[1] end
        m = ngx_re_match(auth, RE_V2, "jo")
        if m then return m[1] end
    end
    local c = args and args["X-Amz-Credential"]
    if type(c) == "string" and c ~= "" then
        local p = find(c, "/", 1, true)
        return p and sub(c, 1, p - 1) or c
    end
    return nil
end

return _M
