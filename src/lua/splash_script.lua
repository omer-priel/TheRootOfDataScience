function main(splash)
  function wait_for(splash, condition)
    while not condition() do
      splash:wait(0.05)
    end
  end

  local btnSelector = 'section[aria-label="Amenities and More"] button[aria-controls]'
  local atterSelector = 'section[aria-label="Amenities and More"] span'

  function atters_extra_exist()
    return splash:evaljs("document.querySelector(\'".. btnSelector .. "\') != null")
  end

  function click_atters()
    local command = "document.querySelector(\'" .. btnSelector .. "\').click();"
    splash:runjs(command)
    return splash:evaljs("document.querySelector(\'" .. btnSelector .. "\').textContent == \'Show Less\'")
  end

  local url = splash.args.url
  assert(splash:go(url))
  assert(splash:wait(0.5))
  wait_for(splash, atters_extra_exist)
  wait_for(splash, click_atters)
  local elements = splash:select_all('yelp-react-root div section[aria-label=\"Amenities and More\"] span')
  local html = splash:html()
  return {command=command, elements=#elements}
end