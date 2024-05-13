package etcd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/magiconair/properties"
	"go.etcd.io/etcd/client/pkg/v3/transport"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	etcdEndpoints         = "etcd.endpoints"
	etcdDialTimeout       = "etcd.dial_timeout"
	etcdCertFile          = "etcd.cert_file"
	etcdKeyFile           = "etcd.key_file"
	etcdCaFile            = "etcd.cacert_file"
	etcdSerializableReads = "etcd.serializable_reads"
)

type etcdCreator struct{}

type etcdDB struct {
	p      *properties.Properties
	client *TcpClient
}

type TcpClient struct {
	Addr string
}

type GetResponse struct {
	Count int64
	Kvs   []*Command
}

const (
	GET uint8 = iota
	PUT
	DELETE
)

type Command struct {
	Op    uint8
	Key   string
	Value string
}

func (cmd *Command) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cmd)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *TcpClient) Get(key string) (GetResponse, error) {
	Conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return GetResponse{}, err
	}
	// encode kvs
	kv := Command{
		Op:    GET,
		Key:   key,
		Value: "",
	}
	buf, err := kv.Encode()
	if err != nil {
		return GetResponse{}, err
	}

	// 发送消息
	_, err = Conn.Write(buf)
	if err != nil {
		return GetResponse{}, err
	}

	// 创建一个足够大的缓冲区来存储数据
	readBuf := make([]byte, 4096)

	// 读取数据
	n, err := Conn.Read(readBuf)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return GetResponse{}, err
	}

	// 使用读取的数据
	data := readBuf[:n]

	// 创建一个新的GetResponse
	var resp GetResponse

	// 创建一个新的解码器
	dec := gob.NewDecoder(bytes.NewBuffer(data))

	// 解码数据
	err = dec.Decode(&resp)
	if err != nil {
		fmt.Println("Error decoding:", err.Error())
		return GetResponse{}, err
	}

	return resp, nil
}

func (c *TcpClient) Put(key string, value string) error {
	Conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return err
	}
	// encode kvs
	kv := Command{
		Op:    PUT,
		Key:   key,
		Value: value,
	}
	buf, err := kv.Encode()
	if err != nil {
		return err
	}

	// 发送消息
	_, err = Conn.Write(buf)

	if err != nil {
		return nil
	}

	// wait for server response
	response := make([]byte, 256)
	Conn.Read(response)
	fmt.Println(string(response))
	return nil
}

func (c *TcpClient) Delete(key string) error {
	Conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return err
	}
	// encode kvs
	kv := Command{
		Op:  DELETE,
		Key: key,
	}
	buf, err := kv.Encode()
	if err != nil {
		return err
	}

	// 发送消息
	_, err = Conn.Write(buf)
	if err != nil {
		return err
	}

	// wait for server response
	response := make([]byte, 256)
	Conn.Read(response)
	fmt.Println(string(response))
	return nil
}

func init() {
	ycsb.RegisterDBCreator("etcd", etcdCreator{})
}

func (c etcdCreator) Create(p *properties.Properties) (ycsb.DB, error) {

	client := TcpClient{
		Addr: "localhost:9360",
	}

	log.Println("--------------------- CREATE ---------------------------")
	return &etcdDB{
		p:      p,
		client: &client,
	}, nil
}

func getClientConfig(p *properties.Properties) (*clientv3.Config, error) {

	log.Println("--------------------- Init ---------------------------")
	endpoints := p.GetString(etcdEndpoints, "localhost:2379")

	fmt.Printf("-----------------------endpoints %s --------------------\n", endpoints)
	dialTimeout := p.GetDuration(etcdDialTimeout, 2*time.Second)

	var tlsConfig *tls.Config
	if strings.Contains(endpoints, "https") {
		tlsInfo := transport.TLSInfo{
			CertFile:      p.MustGetString(etcdCertFile),
			KeyFile:       p.MustGetString(etcdKeyFile),
			TrustedCAFile: p.MustGetString(etcdCaFile),
		}
		c, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}
		tlsConfig = c
	}

	return &clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: dialTimeout,
		TLS:         tlsConfig,
	}, nil
}

func (db *etcdDB) Close() error {
	return nil
	//return db.client.Close()
}

func (db *etcdDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *etcdDB) CleanupThread(_ context.Context) {
}

func getRowKey(table string, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

func (db *etcdDB) Read(ctx context.Context, table string, key string, _ []string) (map[string][]byte, error) {
	rkey := getRowKey(table, key)
	value, err := db.client.Get(rkey)
	if err != nil {
		return nil, err
	}

	if value.Count == 0 {
		return nil, fmt.Errorf("could not find value for key [%s]", rkey)
	}

	var r map[string][]byte
	log.Println("------------------------ read ----------------------------------")
	err = json.NewDecoder(bytes.NewReader([]byte(value.Kvs[0].Value))).Decode(&r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (db *etcdDB) Scan(ctx context.Context, table string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	rkey := getRowKey(table, startKey)
	values, err := db.client.Get(rkey)
	if err != nil {
		return nil, err
	}

	if values.Count != int64(count) {
		return nil, fmt.Errorf("unexpected number of result for key [%s], expected %d but was %d", rkey, count, values.Count)
	}

	for _, v := range values.Kvs {
		var r map[string][]byte
		err = json.NewDecoder(bytes.NewReader([]byte(v.Value))).Decode(&r)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (db *etcdDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rkey := getRowKey(table, key)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	err = db.client.Put(rkey, string(data))
	if err != nil {
		return err
	}

	return nil
}

func (db *etcdDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	//	fmt.Println("-------------------- Insert -------------------------------")
	return db.Update(ctx, table, key, values)
}

func (db *etcdDB) Delete(ctx context.Context, table string, key string) error {
	err := db.client.Delete(getRowKey(table, key))
	if err != nil {
		return err
	}
	return nil
}
