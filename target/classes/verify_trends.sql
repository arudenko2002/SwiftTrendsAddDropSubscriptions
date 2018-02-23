with getDetails AS (
SELECT "spotify_track_history_details" as name,count(*) FROM `umg-dev.swift_alerts.spotify_track_history_details`
WHERE _PARTITIONTIME = "2018-02-04 00:00:00"
LIMIT 1000
),
getAlerts AS (
SELECT "playlist_track_action" as name,count(*) FROM `umg-dev.swift_alerts.playlist_track_action`
WHERE _PARTITIONTIME = "2018-02-04 00:00:00"
LIMIT 1000
),
getEmails AS (
SELECT "playlist_track_action_emails" as name,count(distinct(email)) FROM `umg-dev.swift_alerts.playlist_track_action`
WHERE _PARTITIONTIME = "2018-02-04 00:00:00"
LIMIT 1000
),
getResults AS (
select * from getDetails
UNION ALL
select * from getAlerts
UNION ALL
select * from getEmails
)

select * from getResults
order by name