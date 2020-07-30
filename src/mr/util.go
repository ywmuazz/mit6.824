package mr

type Element interface{}

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
