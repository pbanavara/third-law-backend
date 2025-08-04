-- Read the PDF file content
file = io.open("test.pdf", "rb")
pdf_content = file:read("*all")
file:close()

-- Generate a random boundary
boundary = "------------------------" .. os.time()

-- Create the multipart/form-data request body
request_body = "--" .. boundary .. "\r\n" ..
               "Content-Disposition: form-data; name=\"file\"; filename=\"test.pdf\"\r\n" ..
               "Content-Type: application/pdf\r\n\r\n" ..
               pdf_content .. "\r\n" ..
               "--" .. boundary .. "--\r\n"

-- wrk setup function
function setup(thread)
   thread:set("boundary", boundary)
   thread:set("body", request_body)
end

-- wrk request function
function request()
   return wrk.format("POST", "/api/v1/upload",
   {
      ["Content-Type"] = "multipart/form-data; boundary=" .. boundary,
      ["Accept"] = "application/json"
   }, body)
end 