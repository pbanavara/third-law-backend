-- wrk -t4 -c100 -d30s -s upload.lua http://127.0.0.1:8000
-----------------------------------------------------------
-- 1.  Read the test file once
-----------------------------------------------------------
local pdf     = assert(io.open("test.pdf", "rb"))
local content = pdf:read("*all")
pdf:close()

-----------------------------------------------------------
-- 2.  Boundary helper
-----------------------------------------------------------
local counter = 0
local function make_body()
  counter = counter + 1
  local boundary = ("BOUNDARY-%d"):format(counter)

  local body = ("--%s\r\n" ..
                'Content-Disposition: form-data; name="file"; filename="test.pdf"\r\n' ..
                "Content-Type: application/pdf\r\n\r\n" ..
                "%s\r\n" ..
                "--%s--\r\n")
               :format(boundary, content, boundary)

  local headers = {
    ["Content-Type"]   = "multipart/form-data; boundary=" .. boundary,
    ["Content-Length"] = tostring(#body)
  }

  return wrk.format("POST", "/api/v1/upload", headers, body)
end

request = make_body              -- wrk calls this for every hit

-----------------------------------------------------------
-- 3.  Lightweight counters (no latency math here)
-----------------------------------------------------------
local tally = { total = 0, _200 = 0, _4xx = 0, _5xx = 0, other = 0 }

function response(status, headers, body)
  tally.total = tally.total + 1
  if     status == 200               then tally._200 = tally._200 + 1
  elseif status >= 400 and status<500 then tally._4xx = tally._4xx + 1
  elseif status >= 500               then tally._5xx = tally._5xx + 1
  else   tally.other = tally.other + 1
  end
end

-----------------------------------------------------------
-- 4.  Post-run report (uses wrk’s latency histogram)
-----------------------------------------------------------
local function ms(us) return us / 1000 end         -- µs → ms helper

function done(summary, latency, requests)
  io.write("\n========= Test summary =========\n")
  io.write(("Total requests      : %d\n"):format(tally.total))
  io.write(("  200 OK            : %d\n"):format(tally._200))
  io.write(("  4xx client errors : %d\n"):format(tally._4xx))
  io.write(("  5xx server errors : %d\n"):format(tally._5xx))
  io.write(("  Other statuses    : %d\n\n"):format(tally.other))

  io.write(("Duration            : %.2fs\n"):format(summary.duration/1e6))
  io.write(("Requests / sec      : %.2f\n\n")
           :format(summary.requests / (summary.duration/1e6)))

  io.write("Latency (ms)\n")
  io.write(("  avg   : %.2f\n"):format(ms(latency.mean)))
  io.write(("  p50   : %.2f\n"):format(ms(latency:percentile(50))))
  io.write(("  p75   : %.2f\n"):format(ms(latency:percentile(75))))
  io.write(("  p90   : %.2f\n"):format(ms(latency:percentile(90))))
  io.write(("  p95   : %.2f\n"):format(ms(latency:percentile(95))))
  io.write(("  p99   : %.2f\n"):format(ms(latency:percentile(99))))
  io.write(("  max   : %.2f\n"):format(ms(latency.max)))

  if summary.errors and
     (summary.errors.connect > 0 or summary.errors.read   > 0 or
      summary.errors.write   > 0 or summary.errors.timeout> 0) then
    io.write("\nErrors\n")
    for k,v in pairs(summary.errors) do
      io.write(("  %-7s: %d\n"):format(k, v))
    end
  end
  io.write("================================\n")
end
