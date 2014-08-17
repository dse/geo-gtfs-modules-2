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
		- you can specify a GTFS or GTFS-realtime feed URL.
			- .zip file extension indicates a GTFS feed.
			- .pb file extension indicates a GTFS-realtime feed.
			- other file extension:
				application/x-zip-compressed indicates a GTFS feed.
				application/protobuf indicates a GTFS-realtime feed.
				application/octet-stream:
					you'll have to check the magic on the file after downloading it.
			- if already downloaded, checks for an update.
		--force forces a redownload.
		--all forces all feedsd to be re-checked.

	gtfs2 ridetarc.org update [<url>]
		- updates feeds for current date or later
		- --all forces all feeds to be rechecked.
		- takes --force option as well.

	gtfs2 ridetarc.org update-realtime [--force]

	gtfs2 ridetarc.org realtime-status [--update] [--no-update]

	gtfs2 list-agencies
	gtfs2 [--full] ridetarc.org list-routes

# Local Variables:
# tab-stop-list: (4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64 68 72 76 80 84 88 92 96 100)
# tab-width: 4
# End:
