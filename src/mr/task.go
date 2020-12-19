package mr

type taskState int

const (
	idle taskState = 0
	in_progress taskState = 1
	completed taskState = 2
)

type Task struct {
	Type string
	Filename string
	TaskNum int
	NReduce int
	//当暂时没有idle的任务时，告诉worker放慢请求速度
	EmptyIdle bool
	StartTime int64
}
