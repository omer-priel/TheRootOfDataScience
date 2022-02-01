function main(splash)
  local url = splash.args.url
  assert(splash:go(url))
  assert(splash:wait(0.5))
  local sOpen = "setTimeout(() => {"
  local s1 = "document.querySelector(\'yelp-react-root div section[aria-label=\"Amenities and More\"]"
  local s2 = " button[type=\"submit\"]\').click();"
  local sEnd = "}, 3000);"
  local command = sOpen .. s1 .. s2 .. "document.title='';" .. sEnd
  splash:runjs(command)
  splash:wait(10)
  local elements = splash:select('body')
  local html = splash:html()
  return {command=command, elements=elements:info()}
end