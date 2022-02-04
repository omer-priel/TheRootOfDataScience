cd ..\src
for /l %%i in (0,1,49) do start cmd /c "title %%i&python data_collector.py %%i 50"
