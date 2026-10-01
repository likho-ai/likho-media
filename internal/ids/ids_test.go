package ids

import "testing"

func TestNewIdsAreValidAndSortByTime(t *testing.T) {
	first, second := New("med"), New("med")
	if !Valid(first, "med") || !Valid(second, "med") {
		t.Fatalf("New made invalid ids: %s %s", first, second)
	}
	if first == second {
		t.Fatal("two ids are equal")
	}
	if len(first) != 30 {
		t.Fatalf("id %s is %d characters, want 30", first, len(first))
	}
}

func TestValid(t *testing.T) {
	cases := map[string]bool{
		"med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G":  true,
		"rec_01JB7Z5K3M9Q2W4X6Y8A0C1E3G":  false, // another prefix
		"med_01JB7Z5K3M9Q2W4X6Y8A0C1E3":   false, // too short
		"med_01JB7Z5K3M9Q2W4X6Y8A0C1E3GG": false, // too long
		"med_01jb7z5k3m9q2w4x6y8a0c1e3g":  false, // lower case
		"med_01JB7Z5K3M9Q2W4X6Y8A0C1EIL":  false, // I and L are not in the alphabet
		"med-01JB7Z5K3M9Q2W4X6Y8A0C1E3G":  false,
		"../../etc/passwd":                false,
		"":                                false,
	}
	for id, want := range cases {
		if got := Valid(id, "med"); got != want {
			t.Errorf("Valid(%q) = %v, want %v", id, got, want)
		}
	}
}
