package stream

import "sort"

// AssignWindow 将事件时间分配到长度 size 的对齐滚动窗口 [start, start+size)。
// 对负时间戳同样成立（向下取整对齐）。
func AssignWindow(eventTime, size int64) Window {
	start := eventTime - mod(eventTime, size)
	return Window{Start: start, End: start + size}
}

func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// windowState 保存单个 (key, window) 的聚合中间态。
type windowState struct {
	Count int64
	Sum   int64
}

// windowStore 按分组与窗口保存聚合状态。
type windowStore struct {
	size   int64
	states map[string]map[Window]*windowState
}

func newWindowStore(size int64) *windowStore {
	return &windowStore{size: size, states: make(map[string]map[Window]*windowState)}
}

// add 将事件并入其所属窗口。
func (s *windowStore) add(key string, e Event) {
	w := AssignWindow(e.EventTime, s.size)
	perKey, ok := s.states[key]
	if !ok {
		perKey = make(map[Window]*windowState)
		s.states[key] = perKey
	}
	st, ok := perKey[w]
	if !ok {
		st = &windowState{}
		perKey[w] = st
	}
	st.Count++
	st.Sum += e.Value
}

// fire 触发并移除所有 End <= watermark 的窗口。
// 返回结果按 (Window.End, Window.Start, Key) 确定性排序，保证重放后输出顺序一致。
func (s *windowStore) fire(watermark int64) []WindowResult {
	var out []WindowResult
	for key, perKey := range s.states {
		for w, st := range perKey {
			if w.End <= watermark {
				out = append(out, WindowResult{Key: key, Window: w, Count: st.Count, Sum: st.Sum})
				delete(perKey, w)
			}
		}
		if len(perKey) == 0 {
			delete(s.states, key)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Window.End != b.Window.End {
			return a.Window.End < b.Window.End
		}
		if a.Window.Start != b.Window.Start {
			return a.Window.Start < b.Window.Start
		}
		return a.Key < b.Key
	})
	return out
}

// snapshot 深拷贝当前状态，用于检查点。
func (s *windowStore) snapshot() map[string]map[Window]windowState {
	snap := make(map[string]map[Window]windowState, len(s.states))
	for key, perKey := range s.states {
		m := make(map[Window]windowState, len(perKey))
		for w, st := range perKey {
			m[w] = *st
		}
		snap[key] = m
	}
	return snap
}

// restore 用检查点状态整体替换当前状态。
func (s *windowStore) restore(snap map[string]map[Window]windowState) {
	s.states = make(map[string]map[Window]*windowState, len(snap))
	for key, perKey := range snap {
		m := make(map[Window]*windowState, len(perKey))
		for w, st := range perKey {
			cpy := st
			m[w] = &cpy
		}
		s.states[key] = m
	}
}
