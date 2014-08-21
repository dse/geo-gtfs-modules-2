#!/bin/bash
set -e
gtfs2 () {
    run Geo-GTFS2/bin/gtfs2 "$@"
}
run () {
    echo "+++ $@"
    $@
}

rm -fr ~/.geo-gtfs2/google_transit.sqlite || true

gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transitAUG2014.zip
gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/alerts/Alerts.pb
gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/gtfs-realtime/TrapezeRealTimeFeed.pb
gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/trip_update/TripUpdates.pb
gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/vehicle/VehiclePositions.pb
gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/vehicle/VehiclePositions.pb
#sleep 45
gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/vehicle/VehiclePositions.pb
gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/vehicle/VehiclePositions.pb

cat <<EOF | gtfs2 sqlite
.headers on
select * from geo_gtfs_realtime_feed;
select * from geo_gtfs_realtime_feed_instance;
EOF

