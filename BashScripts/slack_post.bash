#!/bin/bash

###########################################
# Script to post message to SLACK Channel #
###########################################

#Script usage

function usage {
    programName=$0
    echo "description: use this program to post messages to Slack channel"
    echo "usage: $programName [-t \"sample title\"] [-b \"message body\"] [-c \"mychannel\"] [-u \"slack url\"]"
    echo "    -t    the title of the message you are posting"
    echo "    -b    The message body"
    exit 1
}

while getopts ":t:b:c:u:h" opt; do
  case ${opt} in
    t) msgTitle="$OPTARG"
    ;;
    b) msgBody="$OPTARG"
    ;;
    h) usage
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

slackUrl=https://hooks.slack.com/services/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
channelName=datalake-alerts

read -d '' payLoad << EOF
{
        "channel": "#${channelName}",
        "username": "$(hostname)",
        "icon_emoji": ":computer:",
        "attachments": [
            {
                "fallback": "${msgTitle}",
                "color": "good",
                "title": "${msgTitle}",
                "fields": [{
                    "title": "message",
                    "value": "${msgBody}",
                    "short": false
                }]
            }
        ]
    }
EOF


statusCode=$(curl \
        --write-out %{http_code} \
        --silent \
        --output /dev/null \
        -X POST \
        -H 'Content-type: application/json' \
        --data "${payLoad}" ${slackUrl})

echo ${statusCode}
