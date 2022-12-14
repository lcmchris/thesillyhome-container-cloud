# !/bin/bash
echo "Starting to parse DB data"

if  python3 -m thesillyhome.model_creator.main; then
    echo "Starting Appdaemon"
    nohup appdaemon -c /thesillyhome_src/appdaemon/ & 
else
    echo "Model generation failed."
fi
sleep infinity