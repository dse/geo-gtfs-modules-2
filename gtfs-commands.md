    gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transitAUG2014.zip
    gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transitAUG2014.zip
    gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transit%20AUG2012.zip
    gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transitAUG2013.zip
    gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transitAUG2014.zip
    gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transitJAN2013.zip
    gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transitJan2014.zip
    gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transitJUN2013.zip
    gtfs2 ridetarc.org http://googletransit.ridetarc.org/feed/google_transitJUN2014.zip

	gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/alerts/Alerts.pb
	gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/gtfs-realtime/TrapezeRealTimeFeed.pb
	gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/trip_update/TripUpdates.pb
	gtfs2 ridetarc.org http://googletransit.ridetarc.org/realtime/vehicle/VehiclePositions.pb
	
	gtfs2 ridetarc.org <url>
		-	you can specify a GTFS or GTFS-realtime feed URL.
			-	.zip file extension indicates a GTFS feed.
			-	.pb file extension indicates a GTFS-realtime feed.
			-	other file extension:
					application/x-zip-compressed indicates a GTFS feed.
					application/protobuf indicates a GTFS-realtime feed.
					application/octet-stream:
						you'll have to check the magic on the file after downloading it.

	gtfs2 ridetarc.org update-gtfs
	gtfs2 ridetarc.org update-gtfs-realtime
