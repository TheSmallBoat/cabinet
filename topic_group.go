package cabinet

import (
	"fmt"
	"regexp"
	"strings"
)

const _GroupTopicRegexp = `^\$share/([0-9a-zA-Z_-]+)/(.*)$`

var groupCompile = regexp.MustCompile(_GroupTopicRegexp)

func getGroupNameFromTopic(topic []byte) ([]byte, []byte, bool, error) {
	if strings.HasPrefix(string(topic), "$share/") {
		substr := groupCompile.FindStringSubmatch(string(topic))
		if len(substr) != 3 {
			return []byte(""), []byte(""), false, fmt.Errorf("topicGroup/groupNameFromTopic: string match found error => [size %d != 3] ", len(substr))
		}
		groupName := substr[1]
		topic := substr[2]
		return []byte(groupName), []byte(topic), true, nil
	} else {
		return []byte(""), topic, false, nil
	}
}
