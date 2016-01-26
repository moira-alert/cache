package filter

import (
	"fmt"
	"log"
	"path"
	"strings"
	"time"

	"github.com/vova616/xxhash"
)

var asteriskHash = xxhash.Checksum32([]byte("*"))

// PatternStorage contains pattern tree
type PatternStorage struct {
	PatternTree *PatternNode
}

type PatternNode struct {
	Children   []*PatternNode
	Part       string
	Hash       uint32
	Prefix     string
	InnerParts []string
}

// NewPatternStorage creates new PatternStorage struct
func NewPatternStorage() *PatternStorage {
	return &PatternStorage{}
}

// DoRefresh builds pattern tree from redis data
func (t *PatternStorage) DoRefresh(db *DbConnector) error {
	patterns, err := db.getPatterns()
	if err != nil {
		return err
	}

	return t.buildTree(patterns)
}

// Refresh run infinite refresh of patterns tree
func (t *PatternStorage) Refresh(db *DbConnector) {
	for {
		timer := time.Now()
		err := t.DoRefresh(db)
		if err != nil {
			log.Printf("pattern refresh failed: %s", err.Error())
		}
		BuildTreeTimer.UpdateSince(timer)

		time.Sleep(time.Second)
	}
}

func (t *PatternStorage) buildTree(patterns []string) error {
	newTree := &PatternNode{}

	for _, pattern := range patterns {
		currentNode := newTree
		parts := strings.Split(pattern, ".")
		for _, part := range parts {
			found := false
			for _, child := range currentNode.Children {
				if part == child.Part {
					currentNode = child
					found = true
					break
				}
			}
			if !found {
				newNode := &PatternNode{Part: part}

				if currentNode.Prefix == "" {
					newNode.Prefix = part
				} else {
					newNode.Prefix = fmt.Sprintf("%s.%s", currentNode.Prefix, part)
				}

				if part == "*" || !strings.ContainsAny(part, "{*") {
					newNode.Hash = xxhash.Checksum32([]byte(part))
				} else {
					if strings.Contains(part, "{") && strings.Contains(part, "}") {
						prefix, bigSuffix := split2(part, "{")
						inner, suffix := split2(bigSuffix, "}")
						innerParts := strings.Split(inner, ",")

						newNode.InnerParts = make([]string, 0, len(innerParts))
						for _, innerPart := range innerParts {
							newNode.InnerParts = append(newNode.InnerParts, fmt.Sprintf("%s%s%s", prefix, innerPart, suffix))
						}
					} else {
						newNode.InnerParts = []string{part}
					}

				}
				currentNode.Children = append(currentNode.Children, newNode)
				currentNode = newNode
			}
		}
	}

	t.PatternTree = newTree

	return nil
}

// MatchPattern returns array of matched patterns
func (t *PatternStorage) MatchPattern(metric string) []string {
	var (
		currentLevel []*PatternNode
		nextLevel    []*PatternNode
		matched      []string
	)

	currentLevel = append(currentLevel, t.PatternTree)
	parts := strings.Split(metric, ".")
	for _, part := range parts {
		if len(currentLevel) == 0 || len(part) == 0 {
			return matched
		}

		hash := xxhash.Checksum32([]byte(part))
		for _, node := range currentLevel {
			for _, child := range node.Children {
				match := false

				if child.Hash == asteriskHash || child.Hash == hash {
					match = true
				} else if len(child.InnerParts) > 0 {
					for _, innerPart := range child.InnerParts {
						innerMatch, _ := path.Match(innerPart, part)
						if innerMatch {
							match = true
							break
						}
					}
				}

				if match {
					nextLevel = append(nextLevel, child)
				}
			}
		}

		currentLevel = nextLevel
		nextLevel = make([]*PatternNode, 0, 64)
	}

	for _, node := range currentLevel {
		if len(node.Children) == 0 {
			matched = append(matched, node.Prefix)
		}
	}

	return matched
}
