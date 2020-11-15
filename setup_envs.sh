
# create two environments they don't yet exist; make sure they're up to date

lsvirtualenv | grep covid-19-stats-load

if [ $? -eq 1 ]; then
   mkvirtualenv covid-19-stats-load
fi

workon covid-19-stats-load
pip install -r requirements-load.txt

lsvirtualenv | grep covid-19-stats-dbt

if [ $? -eq 1 ]; then
   mkvirtualenv covid-19-stats-dbt
fi

workon covid-19-stats-dbt
pip install -r requirements-dbt.txt

deactivate
