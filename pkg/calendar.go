package pkg

import "math"
import "time"

type activity struct {
	Count int
}

type activityMap map[int]activity

// Trying out a calendar with stats by day, week and month
type calendar struct {
	Daily   activityMap
	Weekly  activityMap
	Monthly activityMap

	Min int64
	Max int64
}

func newCalendar() *calendar {
	c := calendar{}

	c.Daily = make(activityMap)
	c.Weekly = make(activityMap)
	c.Monthly = make(activityMap)
	c.Min = math.MaxInt64
	c.Max = 0

	return &c
}

func punchCalendar(am *activityMap, timestamp int) {
	is, ok := (*am)[timestamp]

	if !ok {
		is = activity{}
		(*am)[timestamp] = is
	}
}

func copyCalendar(am1, am2 activityMap) {
	for k, v := range am2 {
		is, ok := am1[k]
		if ok {
			is.Count += v.Count
		} else {
			am1[k] = v
		}
	}
}

func (c *calendar) addActivity(timestamp int) {
	if *FLAGS.RETENTION != false {
		punchCalendar(&c.Daily, timestamp/(int(time.Hour.Seconds())*24))
		punchCalendar(&c.Weekly, timestamp/(int(time.Hour.Seconds())*24*7))
		punchCalendar(&c.Monthly, timestamp/(int(time.Hour.Seconds())*24*7*30))
	}

	c.Min = int64(math.Min(float64(timestamp), float64(c.Min)))
	c.Max = int64(math.Max(float64(timestamp), float64(c.Max)))
}

func (c *calendar) CombineCalendar(cc *calendar) {
	copyCalendar(c.Daily, cc.Daily)
	copyCalendar(c.Weekly, cc.Weekly)
	copyCalendar(c.Monthly, cc.Monthly)

	c.Min = int64(math.Min(float64(cc.Min), float64(c.Min)))
	c.Max = int64(math.Max(float64(cc.Max), float64(c.Max)))
}
