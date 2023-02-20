#!/bin/bash

/usr/bin/curl https://gbfs.citibikenyc.com/gbfs/en/station_status.json -s | bzip2 -2 > $HOME/station_status/nyc/station_status`date +_%Y-%m-%d_-_%H.%M.%S`.json.bz2
/usr/bin/curl https://gbfs.capitalbikeshare.com/gbfs/en/station_status.json -s | bzip2 -2 > $HOME/station_status/dc/station_status`date +_%Y-%m-%d_-_%H.%M.%S`.json.bz2
/usr/bin/curl https://gbfs.divvybikes.com/gbfs/en/station_status.json -s | bzip2 -2 > $HOME/station_status/chicago/station_status`date +_%Y-%m-%d_-_%H.%M.%S`.json.bz2
/usr/bin/curl https://toronto-us.publicbikesystem.net/ube/gbfs/v1/en/station_status -s | bzip2 -2 > $HOME/station_status/toronto/station_status`date +_%Y-%m-%d_-_%H.%M.%S`.json.bz2
/usr/bin/curl https://gbfs.baywheels.com/gbfs/en/station_status.json -s | bzip2 -2 > $HOME/station_status/sf/station_status`date +_%Y-%m-%d_-_%H.%M.%S`.json.bz2
/usr/bin/curl https://gbfs.bluebikes.com/gbfs/en/station_status.json -s | bzip2 -2 > $HOME/station_status/boston/station_status`date +_%Y-%m-%d_-_%H.%M.%S`.json.bz2
/usr/bin/curl https://gbfs.bcycle.com/bcycle_lametro/station_status.json -s | bzip2 -2 > $HOME/station_status/la/station_status`date +_%Y-%m-%d_-_%H.%M.%S`.json.bz2
/usr/bin/curl https://gbfs.niceridemn.com/gbfs/en/station_status.json -s | bzip2 -2 > $HOME/station_status/minneapolis/station_status`date +_%Y-%m-%d_-_%H.%M.%S`.json.bz2
/usr/bin/curl https://api-core.bixi.com/gbfs/en/station_status.json -s | bzip2 -2 > $HOME/station_status/montreal/station_status`date +_%Y-%m-%d_-_%H.%M.%S`.json.bz2
