package internal

import "testing"

func TestRemoveColumns(t *testing.T) {
	mystrings := []string{"some", "two", "last"}
	mystrings = removeColField(mystrings, 2)
	if len(mystrings) != 2 {
		t.Error("mystrings did not shrink")
	}

	mystrings = removeColField(mystrings, 0)
	if len(mystrings) != 1 {
		t.Error("mystrings did not shrink")
	}

	mystrings = removeColField(mystrings, 0)
	if len(mystrings) != 0 {
		t.Error("mystrings did not shrink")
	}
}
