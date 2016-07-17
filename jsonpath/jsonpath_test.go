package jsonpath

import (
	"io/ioutil"
	"log"
	"testing"
)

func TestJsonPath(t *testing.T) {
	j, err := NewJson([]byte(`
        {
            "a": [
                {
                    "b": 1
                },
                {
                    "b": 2
                }
            ]
        }
    `))
	if err != nil {
		t.Error(err)
	}

	{
		a, err := j.Query("a[0].b")
		if err != nil {
			t.Error(err)
			return
		}
		if v, ok := a.(float64); !ok || int(v) != 1 {
			t.Error("expect a[0].b == 1, real", v)
			return
		}
		log.Println("----------------")
	}

	{
		a, err := j.Query("a[:].b")
		if err != nil {
			t.Error(err)
			return
		}
		b, ok := a.([]interface{})
		if !ok {
			t.Error()
			return
		}
		if v, ok := b[0].(float64); !ok || int(v) != 1 {
			t.Error()
			return
		}
		if v, ok := b[1].(float64); !ok || int(v) != 2 {
			t.Error()
			return
		}
		log.Println("----------------")
	}

	{
		a, err := j.Query("a.(b=1)[0].b")
		if err != nil {
			t.Error(err)
			return
		}
		t.Log(a)
		if v, ok := a.(float64); !ok || int(v) != 1 {
			t.Error(v)
			return
		}
	}
}

func TestLT(t *testing.T) {
	b, err := ioutil.ReadFile("55.json")
	if err != nil {
		t.Error(err)
	}
	myjson, err := NewJson(b)
	if err != nil {
		t.Error(err)
	}

	//t.Log(string(b))
	fd, err := myjson.Query("pageMap.result")
	if err != nil {
		t.Error(err)
	}
	t.Log(fd)
}
