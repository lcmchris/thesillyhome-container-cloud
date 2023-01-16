
# Welcome to the thesillyhome-container-cloud

# Introduction

The idea behind a second repo is to differentiate the local and cloud version. Why would you want a cloud version you might ask?
With an existing local setup, unless you deploy on a sufficiently large machine there is no way to train big models. Because of the limitation the local version actually has limits setup so that your little rasberry pi don't blow up due to insufficient memory or runtime.
Additionally, at some point a generalized model will help drastically with performance.

# Installation steps

  1. Sign up at [The Silly Home](https://thesillyhome.ai/auth)
  2. Download the The Silly Home Container Cloud repo and setup the options.json file.

``` sh
git clone git@github.com:lcmchris/thesillyhome-container-cloud.git

docker volume create thesillyhome_config
# Find the path to the created volume. Docker volume inspect thesillyhome_config or \\wsl$\docker-desktop-data\data\docker\volumes
cp thesillyhome_src\data\config\options.json <path_to_volume>

``` 
  3. Configuration — Edit the option.json file based on your setup.
``` yaml
api_key: Api key that identifies your user. Retrieved from [Profile & Security](https://thesillyhome.ai/app/profile) under API Key Generation.
actuactors_id: List of all actuators that you want The Silly Home to control. Only switches and lights.
db_options: All settings for connecting to the homeassistant database
  db_password: Database password 
  db_username: Database username e.g homeassistant
  db_host:  Database host 192.168.1.100
  db_port:  Database port '3306'
  db_database:  Database name homeassistant, or the sqlite db filename (`home-assistant_v2.db` is the default)
  db_type:  Database type. Only {sqlite,mysql,mariadb,postgres} is supported
ha_options: All settings for connecting to homeassistant. These settings are only required for Homeassistant Container setup.
  ha_url: IP of your homeassistant
  ha_token: Long lived access token. This is required for the Appdaemon.
```


4. Start Container
``` sh
cd ./thesillyhome-container-cloud
docker-compose up -d
```
> On the first startup, this will perform the data processing, data wrangling and model training process through our SageMaker pipeline. 
A connection to the websocket will be made and the model will be returned after the training is complete. This is take around 30 minutes so go grab a coffee and read our latest blog.
   

5. Review Devices on [TheSillyHome Dashboard](https://thesillyhome.ai/app/dashboard).
>Only Devices with enough cases and a high enough performance are enabled. This is shown with a verified symbol. Sit back and let The Silly Home control your lights.
