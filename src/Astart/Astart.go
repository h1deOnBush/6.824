package main

import (
	"fmt"
	"math"
	"sort"
)

type node struct  {
	ele [4][4]int
	depth int
	axis [2]int
	cost float64
	father int
}

func same(n1, n2 node) bool {
	for i:=0; i<len(n1.ele); i++ {
		for j:=0; j<len(n1.ele[0]); j++ {
			if n1.ele[i][j] != n2.ele[i][j] {
				return false
			}
		}
	}
	return true
}

func hasAppear(n node, closed []node) bool {
	for _, n1 := range closed {
		if same(n1, n) {
			return true
		}
	}
	return false
}

func h1(cur, target node) float64 {
	res := 0.0
	for i:=0; i<4; i++ {
		for j:=0; j<4; j++ {
			if cur.ele[i][j] != 0 && cur.ele[i][j]!= target.ele[i][j] {
				res += 1
			}
		}
	}
	return res
}

func h(cur, target node) float64 {
	res := 0.0
	for i:=0; i<4; i++ {
		for j:=0; j<4; j++ {
			num := cur.ele[i][j]
			if num != 0 {
				for i1:=0; i1<4; i1++ {
					for j1:=0; j1<4; j1++ {
						if target.ele[i1][j1] == num {
							res += math.Abs(float64(i-i1))+math.Abs(float64(j-j1))
						}
					}
				}
			}
		}
	}
	return res
}

func move(cur,target node, x,y int, father int, closed *[]node) (node, bool) {
	if cur.axis[0]+x>=0 && cur.axis[0]+x<4 && cur.axis[1]+y>=0 && cur.axis[1]+y<4 {
		var tmpEle [4][4]int
		for i:=0; i<4; i++ {
			for j:=0; j<4; j++ {
				tmpEle[i][j] = cur.ele[i][j]
			}
		}
		tmpEle[cur.axis[0]][cur.axis[1]] = cur.ele[cur.axis[0]+x][cur.axis[1]+y]
		tmpEle[cur.axis[0]+x][cur.axis[1]+y] = 0
		tmp := node {
			ele: tmpEle,
			depth: cur.depth+1,
			axis: [2]int{cur.axis[0]+x, cur.axis[1]+y},
			father: father,
		}
		if !hasAppear(tmp, *closed) {
			tmp.cost = h(tmp, target)+float64(tmp.depth)
			return tmp, true
		}
	}
	return node{}, false
}

func generateNode(cur,target node, father int, closed *[]node) []node {
	res := []node{}
	if tmp, suc := move(cur, target, 1, 0, father, closed); suc {
		res = append(res, tmp)
	}
	if tmp, suc := move(cur, target, 0, -1, father, closed); suc {
		res = append(res, tmp)
	}
	if tmp, suc := move(cur, target, 0, 1, father, closed); suc {
		res = append(res, tmp)
	}
	if tmp, suc := move(cur, target, -1, 1, father, closed); suc {
		res = append(res, tmp)
	}
	return res
}

func show(n node, closed []node)  {
	if n.father != -1 {
		show(closed[n.father], closed)
	}
	for i:=0; i<4; i++ {
		for j:=0 ; j<4; j++ {
			fmt.Printf("%d, ", n.ele[i][j])
		}
		fmt.Println()
	}
	fmt.Println("cost: ", n.cost)
	fmt.Println()
}

func search(root, target node)  {
	var open,closed []node
	open = append(open, root)
	for len(open)>0 {
		cur := open[0]
		if same(cur, target) {
			fmt.Printf("find, total %d steps\n", cur.depth)
			show(cur, closed)
			break
		}
		open = open[1:]
		closed = append(closed, cur)
		father := len(closed)-1
		open = append(open, generateNode(cur, target, father, &closed)...)
		sort.Slice(open, func(i, j int) bool {
			return open[i].cost<open[j].cost
		})
	}
}

func main()  {
	root := node {
		ele: [4][4]int{{5,1,2,4},{9,6,3,8},{13,15,10,11},{14,0,7,12}},
		axis: [2]int{3,1},
		depth: 0,
		father: -1,
	}
	target := node {
		ele: [4][4]int{{1,2,3,4},{5,6,7,8},{9,10,11,12},{13,14,15,0}},
	}
	search(root, target)
}