package main

import (
	"fmt"
	"log"
	"path"
	"strings"
	"time"

	"github.com/vova616/xxhash"
)

var asteriskHash = xxhash.Checksum32([]byte("*"))

type patternStorage struct {
	patternTree *patternNode
}

type patternNode struct {
	children []*patternNode
	part     string
	hash     uint32
	prefix   string
}

func newPatternStorage() *patternStorage {
	return &patternStorage{}
}

func (t *patternStorage) refresh() {
	for {
		err := t.doRefresh()
		if err != nil {
			log.Printf("pattern refresh failed: %s", err.Error())
		}

		time.Sleep(time.Second)
	}
}

func (t *patternStorage) doRefresh() error {
	patterns, err := db.GetPatterns()
	if err != nil {
		return err
	}

	return t.buildTree(patterns)
}

func (t *patternStorage) buildTree(patterns []string) error {
	newTree := &patternNode{}

	for _, pattern := range patterns {
		currentNode := newTree
		parts := strings.Split(pattern, ".")
		for _, part := range parts {
			found := false
			for _, child := range currentNode.children {
				if part == child.part {
					currentNode = child
					found = true
					break
				}
			}
			if !found {
				newNode := &patternNode{part: part}

				if currentNode.prefix == "" {
					newNode.prefix = part
				} else {
					newNode.prefix = fmt.Sprintf("%s.%s", currentNode.prefix, part)
				}

				if part == "*" || !strings.ContainsAny(part, "{*") {
					newNode.hash = xxhash.Checksum32([]byte(part))
				}
				currentNode.children = append(currentNode.children, newNode)
				currentNode = newNode
			}
		}
	}

	t.patternTree = newTree

	return nil
}

func (t *patternStorage) matchPattern(metric string) []string {
	var (
		currentLevel []*patternNode
		nextLevel    []*patternNode
		matched      []string
	)

	currentLevel = append(currentLevel, t.patternTree)
	parts := strings.Split(metric, ".")
	for _, part := range parts {
		if len(currentLevel) == 0 {
			return matched
		}

		hash := xxhash.Checksum32([]byte(part))
		for _, node := range currentLevel {
			for _, child := range node.children {
				match := false

				if child.hash == asteriskHash || child.hash == hash {
					match = true
				} else if child.hash == 0 {
					// child.hash is 0 => this child.part contains * and/or {}
					if strings.Contains(child.part, "{") && strings.Contains(child.part, "}") {
						prefix, bigSuffix := split2(child.part, "{")
						inner, suffix := split2(bigSuffix, "}")
						innerParts := strings.Split(inner, ",")

						for _, innerPart := range innerParts {
							innerMatch, _ := path.Match(fmt.Sprintf("%s%s%s", prefix, innerPart, suffix), part)
							if innerMatch {
								match = true
								break
							}
						}
					} else {
						match, _ = path.Match(child.part, part)
					}
				}

				if match {
					nextLevel = append(nextLevel, child)
				}
			}
		}

		currentLevel = nextLevel
		nextLevel = make([]*patternNode, 0, 64)
	}

	for _, node := range currentLevel {
		if len(node.children) == 0 {
			matched = append(matched, node.prefix)
		}
	}

	return matched
}
