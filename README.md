# Influx Playground

## Build

The playground environment can be build by running
    
    docker compose build

in the command line

## Start InfluxDB

After the build is finished, start InfluxDB by running

    docker compose up -d influxdb

Finally, run the python app by running 

    docker compose up app

If additional dependencies are added to the python app, add them to `app/requirements.txt` and run

    docker compose build app

Then,

    docker compose up app

will again work as before.