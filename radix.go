package radix

import (
	"bytes"
	"container/list"
	"fmt"
	"github.com/ngaut/logging"
	"math/rand"
)

//todo:
// api
// move string<->[]byte conversion outside of lock

// a node of a radix tree
type radNode struct {
	Prefix   []byte // current prefix of the node
	Children []*radNode
	Value    []byte // stored key
	Version  int64
	father   *radNode
	Seq      int64
	Stat     int64
}

const (
	RESULT_COMMON_PREFIX = 0
	RESULT_CONTENT       = 1

	statInMemory = 0
	statLoading  = 2
	statOnDisk   = 4
)

func (self *Radix) beginWriteBatch() {
	self.h.store.BeginWriteBatch()
}

func (self *Radix) commitWriteBatch() error {
	return self.h.store.CommitWriteBatch()
}

func (self *Radix) rollback() error {
	return self.h.store.Rollback()
}

func (self *Radix) getIndex(n *radNode) int {
	for i := 0; i < len(n.father.Children); i++ { //get index
		if n.father.Children[i].Seq == n.Seq {
			return i
		}
	}

	return -1
}

func (self *Radix) pathCompression(n *radNode, leaf *radNode) {
	var latest *radNode
	logging.Infof("pathCompression %+v, %+v", n, leaf)
	if n.Seq == ROOT_SEQ {
		logging.Infof("persistent %+v", n)
		self.h.persistentNode(n, nil)
		return
	}

	prefix := make([]byte, 0)
	for n != nil && n.Seq != ROOT_SEQ && len(n.Children) == 1 && len(n.Value) == 0 {
		latest = n
		prefix = append(n.Prefix, prefix...)
		err := self.h.delNodeFromStorage(n.Seq)
		if err != nil {
			logging.Fatal(err)
		}
		self.h.AddInMemoryNodeCount(-1)

		//cleanup n
		n = n.father
	}

	if latest == nil {
		logging.Infof("persistent %+v", n)
		self.h.persistentNode(n, nil)
		return
	}

	self.h.getChildrenByNode(leaf) //we need to copy child if leaf is on disk

	err := self.h.delNodeFromStorage(leaf.Seq) //no need to dec inmemory node count, because we reuse it
	if err != nil {
		logging.Fatal(err)
	}

	leaf.Prefix = append(prefix, leaf.Prefix...)
	leaf.Seq = latest.Seq
	leaf.father = latest.father

	*latest = *leaf
	adjustFather(latest)

	logging.Infof("persistent %+v, %+v", latest.father, latest)

	self.h.persistentNode(latest, nil)
	self.h.persistentNode(latest.father, nil)
}

func (self *Radix) deleteNode(n *radNode) {
	if n.Seq == ROOT_SEQ { //root
		self.h.persistentNode(n, nil)
		return
	}

	logging.Infof("deleteNode %+v, %+v", n, n.father)
	//remove from storage
	if len(n.Value) > 0 {
		err := self.h.delFromStoragebyKey(n.Value)
		if err != nil {
			logging.Fatal(err)
		}
		n.Value = nil
	}

	logging.Info(n.Seq, n.father.Seq)
	if len(n.Children) > 1 {
		logging.Infof("persistent %+v", n)
		err := self.h.persistentNode(n, nil)
		if err != nil {
			logging.Fatal(err)
		}
		err = self.h.persistentNode(n.father, nil)
		if err != nil {
			logging.Fatal(err)
		}
		return
	} else if len(n.Children) == 1 {
		self.pathCompression(n, n.Children[0])
		return
	}

	//now, n has no children, check if we need to clean father
	//todo: binary search
	i := self.getIndex(n)

	self.h.delNodeFromStorage(n.Seq)
	self.h.AddInMemoryNodeCount(-1)

	//n is leaf node
	if len(n.father.Children) > 1 {
		n.father.Children[i] = nil
		if i == len(n.father.Children)-1 { //last one
			n.father.Children = n.father.Children[:i]
		} else {
			n.father.Children = append(n.father.Children[:i], n.father.Children[i+1:]...)
		}

		if len(n.father.Children) == 1 { //if there is only node after remove, we can do combine
			self.pathCompression(n.father, n.father.Children[0])
			return
		}

		self.h.persistentNode(n.father, nil)
	} else if len(n.father.Children) == 1 {
		n.father.Children[0] = nil
		n.father.Children = nil

		if len(n.father.Value) == 0 {
			logging.Info("recursive delete")
			self.deleteNode(n.father) //recursive find & delete
		} else {
			logging.Infof("persistent %+v, %d", n.father, len(n.father.Value))
			self.h.persistentNode(n.father, nil)
		}
	} else {
		panic("never happend")
	}
}

// implements delete
func (r *radNode) delete(key []byte, tree *Radix) []byte {
	if x, _, ok := r.lookup(key, tree); ok && len(x.Value) > 0 {
		v, err := tree.h.GetValueFromStore(x.Value)
		if err != nil {
			logging.Fatal("never happend")
		}

		// logging.Debugf("delete %s father %+v", key, x.father)
		// logging.Infof("delete %v father %v", x.Seq, father.Seq)

		tree.deleteNode(x)

		return v
	}

	return nil
}

// implements insert or replace, return nil, nil if this a new value
func (r *radNode) put(key []byte, Value []byte, internalKey []byte, version int64, force bool, tree *Radix) ([]byte, error) {
	logging.Info("insert", internalKey, "--", string(Value), r.Prefix)

	tree.h.getChildrenByNode(r)

	for _, d := range r.Children {
		// tree.h.getChildrenByNode(d)
		if len(d.Prefix) == 0 { //check
			panic("never happend")
		}

		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}

		if len(comm) == len(key) {
			if len(comm) == len(d.Prefix) {
				if len(d.Value) == 0 {
					d.Value = internalKey
					tree.h.getChildrenByNode(d)
					tree.h.persistentNode(d, Value) //todo: children seq not changed
					// tree.h.persistentNode(d.father, nil)
					return nil, nil
				}

				if force || version == d.Version {
					d.Value = internalKey
					orgValue, err := tree.h.GetValueFromStore(d.Value)
					if err != nil {
						logging.Fatal(err)
					}
					d.Version++
					tree.h.getChildrenByNode(d)
					tree.h.persistentNode(d, Value) //todo: children seq not changed
					// tree.h.persistentNode(d.father, nil)
					return orgValue, nil
				}

				// logging.Infof("version not match, version is %d, but you provide %d, %+v", d.Version, version, d)
				return nil, fmt.Errorf("key: %s, version not match, version is %d, but you provide %d, %+v", internalKey, d.Version, version, d)
			}

			tree.h.getChildrenByNode(d)

			//ex: ab, insert a
			n := &radNode{
				Prefix:   cloneByteSlice(d.Prefix[len(comm):]),
				Value:    d.Value,
				father:   d,
				Children: d.Children,
				Seq:      tree.h.allocSeq(),
			}
			//adjust father
			adjustFather(n)
			tree.h.AddInMemoryNodeCount(1)

			tree.h.persistentNode(n, nil)

			d.Children = make([]*radNode, 1, 1)
			d.Children[0] = n
			d.Prefix = comm //todo: no need to clone, we can reuse comm
			d.Value = internalKey
			tree.h.persistentNode(d, Value)
			// tree.h.persistentNode(d.father, nil)
			return nil, nil
		}

		//ex: a, insert ab
		if len(comm) == len(d.Prefix) {
			return d.put(key[len(comm):], Value, internalKey, version, force, tree)
		}

		tree.h.getChildrenByNode(d)

		//ex: ab, insert ac, extra common a
		p := &radNode{
			Prefix:   cloneByteSlice(d.Prefix[len(comm):]),
			Value:    d.Value,
			father:   d,
			Children: d.Children,
			Seq:      tree.h.allocSeq(),
		}
		//adjust father
		adjustFather(p)
		tree.h.AddInMemoryNodeCount(1)

		tree.h.persistentNode(p, nil)
		n := &radNode{
			Prefix: cloneByteSlice(key[len(comm):]),
			Value:  internalKey,
			father: d,
			Seq:    tree.h.allocSeq(),
		}
		tree.h.AddInMemoryNodeCount(1)

		tree.h.persistentNode(n, Value)

		d.Prefix = comm //no need to clone, we can reuse comm
		d.Value = nil
		d.Children = make([]*radNode, 2, 2)
		d.Children[0] = p
		d.Children[1] = n

		tree.h.persistentNode(d, nil)
		// tree.h.persistentNode(d.father, nil)
		return nil, nil
	}

	n := &radNode{
		Prefix: cloneByteSlice(key),
		Value:  internalKey,
		father: r,
		Seq:    tree.h.allocSeq(),
	}
	tree.h.AddInMemoryNodeCount(1)
	tree.h.persistentNode(n, Value)
	r.Children = append(r.Children, n)
	tree.h.persistentNode(r, nil)

	return nil, nil
}

func (r *radNode) addToList(l *list.List, tree *Radix) {
	tree.h.getChildrenByNode(r)

	// logging.Infof("checking %+v", r)
	if len(r.Value) > 0 {
		logging.Info("push", r.Value)
		l.PushBack(decodeValueToKey(string(r.Value)))
	}
	for _, d := range r.Children {
		d.addToList(l, tree)
	}
}

type Tuple struct {
	Key   string
	Value string
	Type  int
}

func getWholePrefix(n *radNode, offset int) string {
	var prefix string
	x := n
	for n != nil && n.father != nil {
		prefix = string(n.father.Prefix) + prefix
		n = n.father
	}

	return prefix + string(x.Prefix[:offset])
}

//return: false if full
func save(l *list.List, limitCount int32, currentCount *int32, n *radNode, offset int, tp int, inc bool) bool {
	if inc {
		if *currentCount >= limitCount {
			// logging.Debug("full")
			return false
		}
	}

	if n.Seq != ROOT_SEQ {
		// logging.Debug("save", getWholePrefix(n), n.Value)
		l.PushBack(&Tuple{Key: getWholePrefix(n, offset), Value: string(n.Value), Type: tp})
		if inc {
			*currentCount += 1
		}
	}

	return true
}

func (r *radNode) match(delimiter []byte, limitCount int32, limitLevel int, currentCount *int32, tree *Radix, l *list.List) (goon bool) {
	logging.Info("checking", r.Prefix, "delimiter", delimiter, "value", r.Value)
	if offset := bytes.Index(r.Prefix, delimiter); len(delimiter) > 0 && offset >= 0 {
		logging.Info("delimiter", delimiter, "found")
		save(l, limitCount, currentCount, r, offset+1, RESULT_COMMON_PREFIX, true)
		return false
	}

	if len(r.Value) > 0 { //leaf node
		ok := save(l, limitCount, currentCount, r, len(r.Prefix), RESULT_CONTENT, true)
		if len(r.Children) == 0 || !ok {
			return false
		}
	}

	return true
}

func (r *radNode) listByPrefixDelimiterMarker(skipRoot bool, delimiter []byte, limitCount int32, limitLevel int, currentCount *int32, tree *Radix, l *list.List) {
	logging.Info("level", limitLevel)

	tree.h.getChildrenByNode(r)

	//search root first
	if !skipRoot {
		goon := r.match(delimiter, limitCount, limitLevel, currentCount, tree, l)
		if !goon {
			return
		}
	}

	for _, d := range r.Children {
		//leaf or prefix include delimiter
		tree.h.getChildrenByNode(d)

		goon := d.match(delimiter, limitCount, limitLevel, currentCount, tree, l)
		if !goon {
			continue
		}

		for _, c := range d.Children {
			c.listByPrefixDelimiterMarker(false, delimiter, limitCount, limitLevel+1, currentCount, tree, l)
		}
	}
}

// implementats lookup: node, index, exist
func (r *radNode) lookup(key []byte, tree *Radix) (*radNode, int, bool) {
	tree.h.getChildrenByNode(r)

	if len(key) == 0 {
		return tree.Root, -1, false
	}

	logging.Infof("lookup %s, %+v", key, r)

	for i, d := range r.Children {
		// tree.h.getChildrenByNode(d)

		logging.Infof("lookup %s, %+v", key, d)

		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}

		// The key is found
		if len(comm) == len(key) {
			tree.h.getChildrenByNode(d)
			logging.Infof("found %+v", d)
			if len(comm) == len(d.Prefix) {
				return d, i, true
			}
			return d, i, false
		}

		return d.lookup(key[len(comm):], tree)
	}
	return nil, 0, false
}

func getInMemChildrenCount(n *radNode, cnt *int) { //including root
	*cnt++
	if onDisk(n) {
		return
	}

	for _, c := range n.Children {
		getInMemChildrenCount(c, cnt)
	}
}

func onDisk(n *radNode) bool {
	return n.Stat == statOnDisk
}

func setOnDisk(n *radNode) {
	n.Stat = statOnDisk
	n.Children = nil
}

func cutAll(n *radNode, tree *Radix) int {
	setOnDisk(n)

	cnt := -1 * int(tree.h.GetInMemoryNodeCount())
	tree.h.AddInMemoryNodeCount(cnt)
	return cnt
}

func randomCut(n *radNode, tree *Radix) (retry bool) {
	target := rand.Intn(len(n.Children))

	if onDisk(n.Children[target]) || len(n.Children[target].Children) == 0 {
		return true
	}

	// sum := 0
	// for _, c := range n.Children {
	// 	childrenCnt := 0
	// 	getInMemChildrenCount(c, &childrenCnt)
	// 	logging.Debugf("prefix %s, children count %d", c.Prefix, childrenCnt)
	// 	sum += childrenCnt
	// }

	// //check status
	// if int64(sum) != tree.h.GetInMemoryNodeCount() {
	// 	// tree.h.DumpMemNode(tree.Root, 0)
	// 	logging.Errorf("sum: %d, max: %d, InMemoryNodeCount %d", sum, tree.MaxInMemoryNodeCount, tree.h.GetInMemoryNodeCount())
	// 	panic("")
	// }

	//get children count
	childrenCnt := 0
	getInMemChildrenCount(n.Children[target], &childrenCnt)
	if childrenCnt > 1 {
		logging.Debugf("inmemory: %d, cut prefix %s, childrenCnt %d, father children count %d", tree.h.GetInMemoryNodeCount(),
			n.Children[target].Prefix, childrenCnt, len(n.Children))
		setOnDisk(n.Children[target])
		tree.h.AddInMemoryNodeCount(-childrenCnt + 1) //exclude root node
		return false
	}

	return true
}

func doRandomCut(n *radNode, tree *Radix) int {
	befortCut := tree.h.GetInMemoryNodeCount()
	for i := 0; i < 5; i++ { //max try
		if retry := randomCut(n, tree); !retry {
			break
		}
		logging.Debug("retry")
	}
	afterCut := tree.h.GetInMemoryNodeCount()

	return int(afterCut - befortCut)
}

//remove this tree's children from memory, only cut leaf node
func cutEdge(n *radNode, tree *Radix) int {
	if n == nil || onDisk(n) || len(n.Children) == 0 { //todo: handle only one child
		return 0
	}

	return doRandomCut(n, tree) //cutAll(n, tree)
}

func adjustFather(n *radNode) {
	for _, child := range n.Children {
		child.father = n
	}
}
