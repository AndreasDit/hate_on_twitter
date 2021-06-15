# hate on twitter

## Hosting services

Services I tried:
- Heroku: I was not able to deploy my container in a working state to heroku.
- Kamatera: https://console.kamatera.com. small server with docker installed. super simple and cheap.

## Docker
1. Pull docker image: adtest123/hate_on_twitter
2. docker run -d -it image
3. docker exec -it containername /bin/bash
4. /etc/init.d/cron restart
