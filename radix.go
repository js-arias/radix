package radix

import (
	"container/list"
	"fmt"
	"github.com/ngaut/logging"
	"strings"
)

//todo:
// api
// cut edge, limit count of nodes in memory
// gc performance test

// a node of a radix tree
type radNode struct {
	Prefix   string     `json:"prefix,omitempty"` // current prefix of the node
	Children []*radNode `json:"children,omitempty"`
	Value    string     `json:"value,omitempty"` // stored key
	Version  int64
	father   *radNode
	Seq      int64
	InDisk   bool
}

const (
	RESULT_COMMON_PREFIX = 0
	RESULT_CONTENT       = 1
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

func (self *Radix) pathCompression(n *radNode, leaf *radNode) {
	var prefix string
	var latest *radNode
	logging.Debugf("pathCompression %+v, %+v", n, leaf)
	if n.Seq == ROOT_SEQ {
		logging.Debugf("persistent %+v", n)
		self.h.persistentNode(*n, nil)
		return
	}

	for n != nil && n.Seq != ROOT_SEQ && len(n.Children) == 1 && len(n.Value) == 0 {
		latest = n
		prefix = n.Prefix + prefix
		err := self.h.delNodeFromStorage(n.Seq)
		if err != nil {
			logging.Fatal(err)
		}

		n = n.father
	}

	// if latest == nil {
	// 	logging.Debugf("persistent %+v", n)
	// 	self.h.persistentNode(*n, nil)
	// 	return
	// }

	err := self.h.delNodeFromStorage(leaf.Seq)
	if err != nil {
		logging.Fatal(err)
	}

	father := latest.father

	latest.Prefix = prefix + leaf.Prefix
	latest.Value = leaf.Value
	latest.Version = leaf.Version

	latest.Children[0] = nil
	latest.Children = nil

	logging.Debugf("persistent %+v, %+v", father, leaf)
	self.h.persistentNode(*latest, nil)
	self.h.persistentNode(*father, nil)
}

func (self *Radix) deleteNode(n *radNode) {
	if n.Seq == ROOT_SEQ { //root
		self.h.persistentNode(*n, nil)
		return
	}

	logging.Infof("deleteNode %v %v %+v", n.Seq, n.father.Seq, n)
	//remove from storage
	if len(n.Value) > 0 {
		err := self.h.delFromStoragebyKey(n.Value)
		if err != nil {
			logging.Fatal(err)
		}
		n.Value = ""
	}

	if len(n.Children) > 0 { //todo: combine if only 1 child
		logging.Info(n.Seq, n.father.Seq)
		err := self.h.persistentNode(*n, nil)
		if err != nil {
			logging.Fatal(err)
		}

		return
	}

	//now, n has no children, check if we need to clean father
	//todo: binary search
	i := 0
	for ; i < len(n.father.Children); i++ { //get index
		if n.father.Children[i].Seq == n.Seq {
			break
		}
	}

	self.h.delNodeFromStorage(n.Seq)

	//n is leaf node
	if len(n.father.Children) > 1 {
		n.father.Children[i] = nil
		if i == len(n.father.Children)-1 { //last one
			n.father.Children = n.father.Children[:i]
		} else {
			n.father.Children = append(n.father.Children[:i], n.father.Children[i+1:]...)
		}

		self.h.AddInMemoryNodeCount(-1)

		if len(n.father.Children) == 1 { //if there is only node after remove, we can do combine
			self.pathCompression(n.father, n.father.Children[0])
			return
		}

		self.h.persistentNode(*n.father, nil)
	} else if len(n.father.Children) == 1 {
		n.father.Children[0] = nil
		n.father.Children = nil
		self.h.AddInMemoryNodeCount(-1)

		if len(n.father.Value) == 0 {
			logging.Info("recursive delete")
			self.deleteNode(n.father) //recursive find & delete
		} else {
			logging.Debugf("persistent %+v, %d", n.father, len(n.father.Value))
			self.h.persistentNode(*n.father, nil)
		}
	} else {
		panic("never happend")
	}
}

// implements delete
func (r *radNode) delete(key string, tree *Radix) []byte {
	if x, _, ok := r.lookup(key, tree); ok {
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
func (r *radNode) put(key string, Value []byte, orgKey string, version int64, force bool, tree *Radix) ([]byte, error) {
	logging.Info("insert", orgKey, "--", string(Value))
	if r.InDisk {
		logging.Infof("Load %+v", r)
		tree.h.getChildrenByNode(r)
	}

	for _, d := range r.Children {
		if d.InDisk {
			checkprefix := d.Prefix
			tree.h.getChildrenByNode(d)
			if d.Prefix != checkprefix {
				logging.Fatal("d.Prefix", d.Prefix, checkprefix)
			}
		}

		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}

		if len(comm) == len(key) {
			if len(comm) == len(d.Prefix) {
				if len(d.Value) == 0 {
					d.Value = encodeValueToInternalKey(orgKey)
					tree.h.persistentNode(*d, Value)
					return nil, nil
				}

				if force || version == d.Version {
					d.Value = encodeValueToInternalKey(orgKey)
					orgValue, err := tree.h.GetValueFromStore(d.Value)
					if err != nil {
						logging.Fatal(err)
					}
					d.Version++
					tree.h.persistentNode(*d, Value)
					tree.h.persistentNode(*d.father, nil)
					return orgValue, nil
				}

				// logging.Infof("version not match, version is %d, but you provide %d", d.Version, version)
				return nil, fmt.Errorf("version not match, version is %d, but you provide %d", d.Version, version)
			}

			//ex: ab, insert a
			n := &radNode{
				Prefix:   d.Prefix[len(comm):],
				Value:    d.Value,
				father:   d,
				Children: d.Children,
				Seq:      tree.h.allocSeq(),
			}
			//adjust father
			adjustFather(n)
			tree.h.AddInMemoryNodeCount(1)

			tree.h.persistentNode(*n, nil)

			d.Children = make([]*radNode, 1, 1)
			d.Children[0] = n
			d.Prefix = comm
			d.Value = encodeValueToInternalKey(orgKey)
			tree.h.persistentNode(*d, Value)
			tree.h.persistentNode(*d.father, nil)
			return nil, nil
		}

		//ex: a, insert ab
		if len(comm) == len(d.Prefix) {
			return d.put(key[len(comm):], Value, orgKey, version, force, tree)
		}

		//ex: ab, insert ac, extra common a
		p := &radNode{
			Prefix:   d.Prefix[len(comm):],
			Value:    d.Value,
			father:   d,
			Children: d.Children,
			Seq:      tree.h.allocSeq(),
		}
		//adjust father
		adjustFather(p)
		tree.h.AddInMemoryNodeCount(1)

		tree.h.persistentNode(*p, nil)
		n := &radNode{
			Prefix: key[len(comm):],
			Value:  encodeValueToInternalKey(orgKey),
			father: d,
			Seq:    tree.h.allocSeq(),
		}
		tree.h.AddInMemoryNodeCount(1)

		tree.h.persistentNode(*n, Value)

		d.Prefix = comm
		d.Value = ""
		d.Children = make([]*radNode, 2, 2)
		d.Children[0] = p
		d.Children[1] = n

		tree.h.persistentNode(*d, nil)
		tree.h.persistentNode(*d.father, nil)
		return nil, nil
	}

	n := &radNode{
		Prefix: key,
		Value:  encodeValueToInternalKey(orgKey),
		father: r,
		Seq:    tree.h.allocSeq(),
	}
	tree.h.AddInMemoryNodeCount(1)
	tree.h.persistentNode(*n, Value)
	r.Children = append(r.Children, n)
	tree.h.persistentNode(*r, nil)

	return nil, nil
}

func (r *radNode) addToList(l *list.List, tree *Radix) {
	if r.InDisk {
		tree.h.getChildrenByNode(r)
	}
	// logging.Infof("checking %+v", r)
	if len(r.Value) > 0 {
		logging.Info("push", r.Value)
		l.PushBack(decodeValueToKey(r.Value))
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

//return: false if full
func save(l *list.List, str string, marker string, limitCount int32, currentCount *int32, n *radNode, inc bool) bool {
	if inc {
		if *currentCount >= limitCount {
			// logging.Debug("full")
			return false
		}
	}

	if str > marker {
		tp := RESULT_CONTENT
		if len(n.Value) == 0 {
			tp = RESULT_COMMON_PREFIX
		}
		// logging.Debug("save", str, n.Value)
		l.PushBack(&Tuple{Key: str, Value: n.Value, Type: tp})
		if inc {
			*currentCount += 1
		}
	}

	return true
}

func (r *radNode) listByPrefixDelimiterMarker(marker string, delimiter string, limitCount int32, limitLevel int, currentCount *int32, tree *Radix) *list.List {
	l := list.New()

	if r.InDisk {
		tree.h.getChildrenByNode(r)
	}

	//search tree first
	if pos := strings.Index(r.Prefix, delimiter); pos >= 0 {
		logging.Info("delimiter ", delimiter, " found")
		if !save(l, r.Prefix[:pos+1], marker, limitCount, currentCount, r, true) || len(r.Value) == 0 {
			return l
		}
	}

	n := len(common(marker, r.Prefix))
	marker = marker[n:]

L:
	for _, d := range r.Children {
		//leaf or prefix include delimiter
		if d.InDisk {
			tree.h.getChildrenByNode(d)
		}

		if len(d.Children) == 0 { //leaf node
			logging.Info("check", d.Prefix, d.Value)
			if pos := strings.Index(d.Prefix, delimiter); pos >= 0 {
				logging.Info("delimiter ", delimiter, " found")
				if !save(l, d.Prefix[:pos+1], marker, limitCount, currentCount, d, true) {
					break L
				}

				//no need to search sub tree
				continue
			}

			if !save(l, d.Prefix, marker, limitCount, currentCount, d, true) {
				break L
			}

			continue
		}

		logging.Info("check", d.Prefix, d.Value)

		if pos := strings.Index(d.Prefix, delimiter); pos >= 0 {
			logging.Info("delimiter ", delimiter, " found")
			if !save(l, d.Prefix[:pos+1], marker, limitCount, currentCount, d, true) {
				break L
			}

			//no need to search sub tree
			if len(d.Value) == 0 {
				continue
			}

			n := len(common(marker, r.Prefix))
			ll := d.listByPrefixDelimiterMarker(marker[n:], delimiter, limitCount, limitLevel+1, currentCount, tree)
			for e := ll.Front(); e != nil; e = e.Next() { //no need to check full, already checked by child function
				save(l, e.Value.(*Tuple).Key, marker, limitCount, currentCount, d, false)
			}

		} else {
			if !save(l, d.Prefix, marker, limitCount, currentCount, d, true) {
				break L
			}
			// logging.Debugf("%+v, marker:%s", d, marker)

			n := len(common(marker, r.Prefix))
			ll := d.listByPrefixDelimiterMarker(marker[n:], delimiter, limitCount, limitLevel+1, currentCount, tree)
			for e := ll.Front(); e != nil; e = e.Next() { //no need to check full, already checked by child function
				save(l, e.Value.(*Tuple).Key, marker, limitCount, currentCount, d, false)
			}
		}
	}

	moreCompleteList := list.New()
	for e := l.Front(); e != nil; e = e.Next() {
		// println("level:", limitLevel, "moreCompleteList", r.Prefix+e.Value.(string))
		e.Value.(*Tuple).Key = r.Prefix + e.Value.(*Tuple).Key
		moreCompleteList.PushBack(e.Value.(*Tuple))
	}

	return moreCompleteList
}

// implementats lookup: node, index, exist
func (r *radNode) lookup(key string, tree *Radix) (*radNode, int, bool) {
	if r.InDisk {
		tree.h.getChildrenByNode(r)
		// logging.Infof("get from disk %+v, searching %s", r, key)
	}

	logging.Info("lookup", key)
	for i, d := range r.Children {
		if d.InDisk { //if we need children, we need to load from disk
			tree.h.getChildrenByNode(d)
			logging.Infof("get from disk %+v, searching %s", d, key)
		}

		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}
		// The key is found
		if len(comm) == len(key) {
			if len(comm) == len(d.Prefix) {
				// logging.Info("found", d.Value)
				return d, i, true
			}
			return d, i, false
		}
		return d.lookup(key[len(comm):], tree)
	}
	return nil, 0, false
}

//remove this tree's children from memory
func cutEdge(n *radNode, tree *Radix) {
	if n == nil {
		return
	}

	indisk := false

	if len(n.Children) > 0 {
		indisk = true
	}

	for i, node := range n.Children {
		cutEdge(node, tree)

		logging.Info("cut seq", n.Children[i].Seq, "internal key", n.Children[i].Value, "father seq", n.Children[i].father.Seq)
		n.Children[i].father = nil
		n.Children[i] = nil
	}

	tree.h.AddInMemoryNodeCount(-len(n.Children))

	n.Children = nil
	n.InDisk = indisk
}

func adjustFather(n *radNode) {
	for _, child := range n.Children {
		child.father = n
	}
}
