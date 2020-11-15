
# ubuntu 20.04
source /usr/share/virtualenvwrapper/virtualenvwrapper.sh

#### extract

workon covid-19-stats-load

make extract

#### load

make load

#### transform

workon covid-19-stats-dbt

make transform

#### export

workon covid-19-stats-load

make export
