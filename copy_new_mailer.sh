if [ "$1" = 1 ]; then
scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/target/SwiftTrendSubscriptions-0.1.jar alexey.rudenko2002@swift-airflow.umusic.net:/tmp/
fi
if [ "$2" = 2 ]; then
scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/resources/track_alert_subscription.py alexey.rudenko2002@swift-airflow.umusic.net:/tmp/
fi
