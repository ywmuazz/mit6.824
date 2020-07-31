package mr

import "encoding/json"

type Element interface{}
type MapData map[string]interface{}

func copySlice(s []Element) []Element {
	ret := []Element{}
	ret = append(ret, s...)
	return ret
}
func copyStringSlice(s []string) []string {
	ret := []string{}
	ret = append(ret, s...)
	return ret
}

func (m MapData) GetString(key string) string {
	value, exist := m[key]
	if !exist {
		return ""
	}
	str, ok := value.(string)
	if !ok {
		return ""
	}
	return str
}

func (m MapData) GetMapData(key string) MapData {
	value, exist := m[key]
	if !exist {
		return make(map[string]interface{}, 0)
	}
	str, ok := value.(map[string]interface{})
	if !ok {
		return make(map[string]interface{}, 0)
	}
	return str
}

func (m MapData) GetInt(key string) int {
	value, exist := m[key]
	if !exist {
		return 0
	}
	ret, ok := value.(int)
	if !ok {
		return 0
	}
	return ret
}

func (m MapData) GetKVlist(key string) []KeyValue {
	value, exist := m[key]
	if !exist {
		return []KeyValue{}
	}
	ret, ok := value.([]KeyValue)
	if !ok {
		return []KeyValue{}
	}
	return ret
}

func (m MapData) GetStringSlice(key string) []string {
	value, exist := m[key]
	if !exist {
		return []string{}
	}
	ret, ok := value.([]string)
	if !ok {
		return []string{}
	}
	return ret
}

func JsonString(x interface{}) string {
	b, err := json.Marshal(x)
	if err != nil {
		return ""
	}
	return string(b)
}
