package task

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SKatiyar/qr"
	"github.com/xlvector/dama2"
	"github.com/xlvector/dlog"
	"github.com/xlvector/higgs/casperjs"
	"github.com/xlvector/higgs/config"
	"github.com/xlvector/higgs/context"
	"github.com/xlvector/higgs/extractor"
	"github.com/xlvector/higgs/jsonpath"
	"github.com/xlvector/higgs/util"
)

type Require struct {
	File string `json:"file"`
	From string `json:"from"`
	To   string `json:"to"`
}

type Retry struct {
	MaxTimes     int  `json:"max_times"`
	ContinueThen bool `json:"continue_then"`
}

type QRCodeImage struct {
	Src        string `json:"src"`
	ContextKey string `json:"context_key"`
}

type Captcha struct {
	CodeType   string `json:"code_type"`
	ImgFormat  string `json:"img_format"`
	ContextKey string `json:"context_key"`
}

type UploadImage struct {
	ContextKey string `json:"context_key"`
	Format     string `json:"format"`
	Base64Src  string `json:"base64_src"`
}

func (p *UploadImage) Filename() string {
	return p.ContextKey + "." + p.Format
}

type HbaseInfo struct {
	//filePath:="/home/kevin/higgs/data/10086/2016/06/20/1466405475831137786/detailnetworknfojsonp_201606.json"
	//dataName :="data"
	//phoneType := "10010"
	//family := "m"
	//formatStr:="01-02 15:04:05"
	//timeKey:="startTime"
	//SendToHbase(filePath,dataName,"15802277329",phoneType,family,formatStr,timeKey)

	FileName  string `json:"file_name"`
	DataName  string `json:"data_name"`
	Phone     string `json:"phone"`
	PhoneType string `json:"phone_type"`
	Family    string `json:"family"`
	FormatStr string `json:"format_str"`
	TimeKey   string `json:"time_key"`
	DataYear  string `json:"data_year"`
	SaveType  string `json:"save_type"`
	Website   string `json:"website"`
}

type Step struct {
	Require         *Require               `json:"require"`
	Tag             string                 `json:"tag"`
	Retry           *Retry                 `json:"retry"`
	CookieJar       string                 `json:"cookiejar"`
	Condition       string                 `json:"condition"`
	NeedParam       string                 `json:"need_param"`
	Page            string                 `json:"page"`
	Method          string                 `json:"method"`
	Header          map[string]string      `json:"header"`
	Params          map[string]string      `json:"params"`
	Actions         []*Action              `json:"actions"`
	JsonPostBody    interface{}            `json:"json_post_body"`
	UploadImage     *UploadImage           `json:"upload_image"`
	Captcha         *Captcha               `json:"captcha"`
	QRcodeImage     *QRCodeImage           `json:"qrcode_image"`
	DocType         string                 `json:"doc_type"`
	OutputFilename  string                 `json:"output_filename"`
	ContextOpers    []string               `json:"context_opers"`
	ExtractorSource string                 `json:"extractor_source"`
	Extractor       map[string]interface{} `json:"extractor"`
	Sleep           int                    `json:"sleep"`
	HbaseInfomation *HbaseInfo             `json:"hbase_info"`
	Message         map[string]string
}

func (s *Step) getPageUrls(c *context.Context) string {
	return c.Parse(s.Page)
}

func (s *Step) getParams(c *context.Context) map[string]string {
	ret := make(map[string]string)
	for k, v := range s.Params {
		ret[c.Parse(k)] = c.Parse(v)
	}
	return ret
}

func (s *Step) addContextOutputs(c *context.Context) {
	for _, co := range s.ContextOpers {
		c.Parse(co)
	}
}

func (s *Step) extract(body []byte, d *Downloader) {
	if s.Extractor == nil || len(s.Extractor) == 0 {
		return
	}
	if len(s.ExtractorSource) > 0 {
		body = []byte(d.Context.Parse(s.ExtractorSource))
	}
	ret, err := extractor.Extract(body, s.Extractor, s.DocType, d.Context)
	if err != nil {
		dlog.Warn("extract error of %v: %v", s.Extractor, err)
		return
	}
	d.AddExtractorResult(ret)
}

func (s *Step) getHeader(c *context.Context) map[string]string {
	ret := make(map[string]string)
	for k, v := range s.Header {
		ret[c.Parse(k)] = c.Parse(v)
	}
	return ret
}

func (s *Step) getRawPostData() []byte {
	b, _ := json.Marshal(s.JsonPostBody)
	return b
}

func (s *Step) download(d *Downloader) ([]byte, error) {
	page := s.getPageUrls(d.Context)
	dlog.Info("download %s", page)
	d.UpdateCookieToContext(page)
	if len(s.Method) == 0 || s.Method == "GET" {
		return d.Get(page, s.getHeader(d.Context))
	} else if s.Method == "POST" {
		return d.Post(page, s.getParams(d.Context), s.getHeader(d.Context))
	} else if s.Method == "POSTJSON" {
		return d.PostRaw(page, s.getRawPostData(), s.getHeader(d.Context))
	}
	return nil, errors.New("unsupported method: " + s.Method)
}

func (s *Step) passCondition(c *context.Context) bool {
	if len(s.Condition) == 0 {
		return true
	}
	return c.Parse(s.Condition) == "true"
}

func (s *Step) GetAction(c *context.Context) *Action {
	for _, f := range s.Actions {
		if f.IsFire(c) {
			return f
		}
	}
	return nil
}

func (s *Step) GetOutputFilename(c *context.Context) string {
	if len(s.OutputFilename) == 0 {
		return ""
	}
	return c.Parse(s.OutputFilename)
}

func (s *Step) procUploadImage(body []byte, d *Downloader) error {
	b := body
	if len(s.UploadImage.Base64Src) > 0 {
		bsrc := d.Context.Parse(s.UploadImage.Base64Src)
		b, _ = base64.StdEncoding.DecodeString(bsrc)
	}
	imgLink, err := util.UploadBody(b, d.OutputFolder+"/"+s.UploadImage.Filename(), CAPTCHA_BUCKET)
	if err != nil {
		dlog.Warn("upload image fail: %v", err)
		return err
	}
	dlog.Info("upload image to %s", imgLink)
	d.Context.Set(s.UploadImage.ContextKey, imgLink)
	return nil
}

func (s *Step) Do(d *Downloader, dm *dama2.Dama2Client, cas *casperjs.CasperJS) error {
	if !s.passCondition(d.Context) {
		return nil
	}

	if len(s.CookieJar) > 0 {
		d.SetCookie(d.Context.Parse(s.CookieJar))
	}

	body := []byte{}
	if len(s.Page) > 0 {
		var err error
		body, err = s.download(d)
		if err != nil {
			return err
		}
	}

	//output file name should calculated before context operations
	out := s.GetOutputFilename(d.Context)
	d.Context.Set("_body", string(body))
	s.addContextOutputs(d.Context)
	s.extract(body, d)

	if len(out) > 0 {
		dlog.Info("write file %s to %s", out, d.OutputFolder+"/"+out)
		err := ioutil.WriteFile(d.OutputFolder+"/"+out, body, 0655)
		if err != nil {
			dlog.Warn("write file failed: %v", err)
		}

		if s.HbaseInfomation != nil {
			//strings.TrimSpace(string(body))
			//dlog.Info("  -> %s",body)
			dlog.Info("  -> %s", s.HbaseInfomation.DataName)
			dlog.Info("  -> %s", d.Context.Parse(s.HbaseInfomation.Phone))
			dlog.Info("  -> %s", s.HbaseInfomation.PhoneType)
			dlog.Info("  -> %s", s.HbaseInfomation.Family)
			dlog.Info("  -> %s", s.HbaseInfomation.FormatStr)
			dlog.Info("  -> %s", s.HbaseInfomation.TimeKey)
			s.saveToHbase(body, d)
		}
	}

	if s.UploadImage != nil {
		s.procUploadImage(body, d)
	}

	if s.QRcodeImage != nil {
		qc, qerr := qr.Encode(d.Context.Parse(s.QRcodeImage.Src), qr.M)
		if qerr != nil {
			dlog.Warn("Encode Qrcode Err:%s", qerr.Error())
		} else {
			png := qc.PNG()
			uploadUrl, err := util.UploadBody(png, d.OutputFolder+"/qrcode.png", CAPTCHA_BUCKET)
			if err != nil {
				dlog.Warn("upload image err:%s", err.Error())
			}
			d.Context.Set(s.QRcodeImage.ContextKey, uploadUrl)
		}
	}

	if s.Captcha != nil && dm != nil {
		ct, _ := strconv.Atoi(s.Captcha.CodeType)
		cret, err := dm.Captcha(body, s.Captcha.ImgFormat, ct, config.Instance.Captcha.AppId, config.Instance.Captcha.Username, config.Instance.Captcha.Password)
		if err != nil {
			dlog.Warn("decode captcha error : %v", err)
		}
		d.Context.Set(s.Captcha.ContextKey, cret)
	}

	if s.Sleep > 0 {
		time.Sleep(time.Duration(s.Sleep) * time.Second)
	}
	return nil
}

func reverse(s string) string {
	length := len(s)
	b := make([]byte, length)
	for i := 0; i < length; i++ {
		b[length-1-i] = s[i]
	}
	return string(b)
}
func formatJson(jsonByte []byte, dataName, phoneNumber, phoneType, family, formatStr, timeKey, dataYear string) map[string]interface{} {
	myJson, err := jsonpath.NewJson(jsonByte)
	finalPostData := make(map[string]interface{})
	if err == nil {
		// row list
		rowDataList := make([]map[string]interface{}, 0)
		fullData, err := myJson.Query(dataName)
		if err != nil {
			dlog.Warn("parse dataNmae fail! %v", err)
		}
		dlog.Info("------------>%s", string(dataName))
		dlog.Info("------------>%v", fullData)
		if err == nil && fullData != nil {
			for i := 0; i < len(fullData.([]interface{})); i++ {
				// row
				rowData := make(map[string]interface{})
				// rowkey
				rowkey := reverse(phoneNumber) + "_" + family + "_" + dataYear
				data, _ := myJson.Query(dataName + "[" + strconv.Itoa(i) + "]")
				dataDetail := data.(map[string]interface{})
				fmt.Println(dataDetail)
				detailList := make([]map[string]interface{}, 0)
				for key, val := range dataDetail {
					//fmt.Println(key)
					detailMap := make(map[string]interface{})
					detailMap["column"] = base64.StdEncoding.EncodeToString([]byte(family + ":" + key))
					if val != nil {
						detailMap["$"] = base64.StdEncoding.EncodeToString([]byte(val.(string)))
					} else {
						detailMap["$"] = nil
					}
					detailList = append(detailList, detailMap)
				}
				typeMap := make(map[string]interface{})
				typeMap["column"] = base64.StdEncoding.EncodeToString([]byte(family + ":type"))
				typeMap["$"] = base64.StdEncoding.EncodeToString([]byte(phoneType))
				detailList = append(detailList, typeMap)
				t, err := time.Parse(formatStr, dataDetail[timeKey].(string))
				if err != nil {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					rowkey += "9"
					for i := 0; i < 7; i++ {
						rowkey += strconv.Itoa(r.Intn(10))
					}
					fmt.Println("time error")
				} else {
					if len(t.Format("20060102150405")) == 14 {

					}
					rowkey += t.Format("0102150405")
				}
				rowData["key"] = base64.StdEncoding.EncodeToString([]byte(rowkey))
				rowData["Cell"] = detailList
				rowDataList = append(rowDataList, rowData)
			}
		}

		finalPostData["Row"] = rowDataList

		dlog.Info("--->>>>%s", finalPostData)
	} else {
		//fmt.Println(err)
		dlog.Warn("format fail! %v", err)
	}
	return finalPostData
}
func (s *Step) sendToHbase(jsonByte []byte, dataName, phoneNumber, phoneType, family, formatStr, timeKey, dataYear string) {
	finalPostData := formatJson(jsonByte, dataName, phoneNumber, phoneType, family, formatStr, timeKey, dataYear)
	str, err := json.Marshal(finalPostData)

	if err != nil {
		dlog.Error("===>sendbase fail! %v %v %v", phoneNumber, family, err)
		return
	}

	go func() {
		client := http.DefaultClient
		resp, err := client.Post("http://g1-bdp-hdp-04:9527/test:user_tel_detail/false-row-key", "application/json", strings.NewReader(string(str)))
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}

		if err != nil {
			dlog.Error("===>habse send fail! %v %v %v", phoneNumber, family, err)
		} else {
			dlog.Info("===>habse send status: %v", resp.Status)
			if resp.StatusCode != 200 {
				b := make([]byte, 4096)
				n, err := resp.Body.Read(b)
				if err != nil {
					dlog.Warn("==>failinfo: %v", err)
				} else if n > 0 {
					dlog.Warn("==>failinfo: %v", bytes.Replace(b[:n], []byte("\n"), []byte(""), -1))
				}
			}
		}
	}()
}

func (s *Step) saveTextToHbase(d *Downloader) {
	hi := s.HbaseInfomation
	data := d.Context.Parse(hi.DataName)
	if len(data) == 0 {
		dlog.Info("data empty: %v", hi)
		return
	}

	site := d.Context.Parse(hi.Website)
	texttype := d.Context.Parse(hi.PhoneType)
	username := d.Context.Parse(hi.Phone)
	tdate := time.Now().Format("20060102")

	row_key := username + "_" + site + "_" + texttype + "_" + tdate
	dlog.Info("row_key====>%v", row_key)

	postdata := make(map[string]interface{})
	rowdata := make(map[string]interface{})
	celldata := make([]map[string]interface{}, 0)
	cell := make(map[string]interface{})
	rows := make([]map[string]interface{}, 0)

	cell["$"] = base64.StdEncoding.EncodeToString([]byte(data))
	cell["column"] = base64.StdEncoding.EncodeToString([]byte(hi.Family + ":" + "file"))
	celldata = append(celldata, cell)
	rowdata["Cell"] = celldata
	rowdata["key"] = base64.StdEncoding.EncodeToString([]byte(row_key))

	rows = append(rows, rowdata)
	postdata["Row"] = rows

	go func() {
		str, err := json.Marshal(postdata)
		if err != nil {
			dlog.Warn("Marshal file fail! %v", row_key)
			return
		}
		dlog.Info("%v", string(str))
		client := http.DefaultClient
		resp, err := client.Post("http://g1-bdp-hdp-04:9527/test:html/false-row-key", "application/json", bytes.NewReader(str))
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}

		if err != nil {
			dlog.Warn("file to hbase fail! %v", row_key)
		} else {
			b := make([]byte, 10240)
			resp.Body.Read(b)
			dlog.Info("filetohbase: %v %v %v", resp.Status, string(b), row_key)
		}
	}()
}

func (s *Step) saveToHbase(body []byte, d *Downloader) {
	if s.HbaseInfomation == nil {
		return
	}

	if s.HbaseInfomation.SaveType == "file" {
		s.saveTextToHbase(d)
	} else {
		bodyStr := string(body)
		bodyStr = strings.TrimSpace(bodyStr)
		bodyStr = bodyStr[strings.Index(bodyStr, "{") : strings.LastIndex(bodyStr, "}")+1]

		s.sendToHbase([]byte(bodyStr), s.HbaseInfomation.DataName, d.Context.Parse(s.HbaseInfomation.Phone),
			s.HbaseInfomation.PhoneType, s.HbaseInfomation.Family, s.HbaseInfomation.FormatStr,
			s.HbaseInfomation.TimeKey, d.Context.Parse(s.HbaseInfomation.DataYear))
	}
}
