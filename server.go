package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

var (
	sm          *http.ServeMux
	redisClient *redigo.Pool
)

const ()

// Batch 优惠券批次
type Batch struct {
	Id     int64  `json:"id"`
	Name   string `json:"name"`
	Value  int64  `json:"value"`
	Amount int64  `json:"amount"`
	Issued bool   `json:"issued"`
}

type BatchSucResp struct {
	Success bool    `json:"success"`
	Data    []Batch `json:"data"`
}

type BatchFailResp struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// Own 拥有优惠券关系
type Own struct {
	Id           int64 `json:"id"`
	UserId       int64 `json:"userId"`
	BatchId      int64 `json:"batchId"`
	Amount       int64 `json:"amount"`
	AcquiredTime int64 `json:"acquiredTime"`
}

// User 用户
type User struct {
	Id          int64  `json:"id"`
	Nickname    string `json:"nickname"`
	PhoneNumber string `json:"phoneNumber"`
}

type CouponStatistic struct {
	Name         string `json:"name"`
	Value        int64  `json:"value"`
	Amount       int64  `json:"amount"`
	AcquiredTime int64  `json:"acquiredTime"`
}

type UserResp struct {
	User             User              `json:"user"`
	CouponStatistics []CouponStatistic `json:"couponStatistics"`
}

type UserSucResp struct {
	Success bool       `json:"success"`
	Data    []UserResp `json:"data"`
}

type UserFailResp struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// splitAmount: return userId_list, amount_list
func splitAmount(totalAmount int64, UserLen int64) ([]int64, []int64) {

	splitUserCnt := rand.Int63n(5) + 1
	if UserLen < splitUserCnt {
		splitUserCnt = UserLen
	}
	amount_list := make([]int64, 0, int(splitUserCnt))
	userId_list := make([]int64, int(UserLen))

	for ind := range userId_list {
		userId_list[ind] = int64(ind)
	}
	rand.Shuffle(int(UserLen), func(i, j int) {
		userId_list[i], userId_list[j] = userId_list[j], userId_list[i]
	})

	restAmount := totalAmount
	for restAmount > 0 {
		curAmount := rand.Int63n(totalAmount) + 1
		if curAmount > restAmount || len(amount_list) == int(splitUserCnt-1) {
			curAmount = restAmount
		}
		amount_list = append(amount_list, curAmount)
		// userId_list = append(userId_list, rand.Int63n(UserLen))
		restAmount -= curAmount
	}
	userId_list = userId_list[0:len(amount_list)]
	return userId_list, amount_list
}

func initServerMux() {
	sm = http.NewServeMux()
	sm.HandleFunc("/coupon-batches", func(resp http.ResponseWriter, req *http.Request) {
		if req.Method == "GET" {
			//查看优惠券列表

			redis := redisClient.Get()
			defer redis.Close()

			reply, err := redigo.Values(redis.Do("sort", "Batch", "get", "Batch:*->id", "get", "Batch:*->name", "get", "Batch:*->value", "get", "Batch:*->amount", "get", "Batch:*->issued"))
			if err != nil {
				fmt.Println("err", err)
			}
			const _itemLen = 5
			BatchLen := len(reply) / _itemLen
			Batch_list := make([]Batch, BatchLen)
			for batchInd := 0; batchInd < BatchLen; batchInd++ {
				id, _ := redigo.Int64(reply[batchInd*_itemLen+0], err)
				name, _ := redigo.String(reply[batchInd*_itemLen+1], err)
				value, _ := redigo.Int64(reply[batchInd*_itemLen+2], err)
				amount, _ := redigo.Int64(reply[batchInd*_itemLen+3], err)
				issued, _ := redigo.Bool(reply[batchInd*_itemLen+4], err)
				Batch_list[batchInd] = Batch{
					Id:     id,
					Name:   name,
					Value:  value,
					Amount: amount,
					Issued: issued,
				}
			}

			batchSucResp := BatchSucResp{
				Success: true,
				Data:    Batch_list,
			}
			jsonData, _ := json.Marshal(batchSucResp)
			resp.Write(jsonData)
		} else if req.Method == "POST" {
			//添加新的优惠券批次

			redis := redisClient.Get()
			defer redis.Close()

			bodyData, err := ioutil.ReadAll(req.Body)
			if err != nil {
				fmt.Println("err", err)
			}
			var batch Batch
			err = json.Unmarshal(bodyData, &batch)
			if err != nil {
				fmt.Println("err", err)
			}
			batchLen, err := redigo.Int64(redis.Do("LLEN", "Batch"))
			if err != nil {
				fmt.Println("err", err)
			}
			batch.Id = batchLen
			batch.Issued = false
			redis.Do("rpush", "Batch", batch.Id)
			redis.Do("hmset", "Batch:"+strconv.Itoa(int(batch.Id)), "id", batch.Id, "name", batch.Name, "value", batch.Value, "amount", batch.Amount, "issued", batch.Issued)
			resp.WriteHeader(200)

		} else if req.Method == "PATCH" {
			// 发放所有优惠券

			redis := redisClient.Get()
			defer redis.Close()

			reply, err := redigo.Values(redis.Do("sort", "Batch", "get", "Batch:*->id", "get", "Batch:*->name", "get", "Batch:*->value", "get", "Batch:*->amount", "get", "Batch:*->issued"))
			if err != nil {
				fmt.Println("err", err)
			}
			const _itemLen = 5
			BatchLen := len(reply) / _itemLen
			Batch_list := make([]Batch, BatchLen)
			for batchInd := 0; batchInd < BatchLen; batchInd++ {
				id, _ := redigo.Int64(reply[batchInd*_itemLen+0], err)
				name, _ := redigo.String(reply[batchInd*_itemLen+1], err)
				value, _ := redigo.Int64(reply[batchInd*_itemLen+2], err)
				amount, _ := redigo.Int64(reply[batchInd*_itemLen+3], err)
				issued, _ := redigo.Bool(reply[batchInd*_itemLen+4], err)
				Batch_list[batchInd] = Batch{
					Id:     id,
					Name:   name,
					Value:  value,
					Amount: amount,
					Issued: issued,
				}
			}

			UserLen, err := redigo.Int64(redis.Do("LLEN", "User"))
			if err != nil {
				fmt.Println("err", err)
			}
			if UserLen == 0 {
				fmt.Printf("error: UserLen == 0\n")
				return
			}
			for _, batch := range Batch_list {
				if !batch.Issued {

					userId_list, amount_list := splitAmount(batch.Amount, UserLen)
					for userInd := range userId_list {
						OwnLen, err := redigo.Int64(redis.Do("LLEN", "Own"))
						if err != nil {
							fmt.Println("err", err)
						}
						own := Own{
							Id:           OwnLen,
							UserId:       userId_list[userInd],
							BatchId:      batch.Id,
							Amount:       amount_list[userInd],
							AcquiredTime: time.Now().Unix(),
						}
						redis.Do("rpush", "Own", own.Id)
						redis.Do("hmset", "Own:"+strconv.Itoa(int(own.Id)), "id", own.Id, "userId", own.UserId, "batchId", own.BatchId, "amount", own.Amount, "acquiredTime", own.AcquiredTime)
					}
					redis.Do("hmset", "Batch:"+strconv.Itoa(int(batch.Id)), "issued", 1)

				}
			}
			resp.WriteHeader(200)

		}

	})
	sm.HandleFunc("/coupon-batches/", func(resp http.ResponseWriter, req *http.Request) {

		if req.Method == "PATCH" {
			// 发放单个批次的优惠券
			// fmt.Println(req.RequestURI)

			redis := redisClient.Get()
			defer redis.Close()

			batchId := strings.Split(req.URL.Path, "/")[2]
			UserLen, err := redigo.Int64(redis.Do("LLEN", "User"))
			if err != nil {
				fmt.Println("err", err)
			}
			if UserLen == 0 {
				fmt.Printf("error: UserLen == 0\n")
				return
			}

			var reply []interface{}
			reply, err = redigo.Values((redis.Do("hmget", "Batch:"+batchId, "id", "name", "value", "amout", "issued")))

			if err != nil {
				fmt.Println("err", err)
			}
			batch := Batch{}
			{
				batch.Id, _ = redigo.Int64(reply[0], err)
				batch.Name, _ = redigo.String(reply[1], err)
				batch.Value, _ = redigo.Int64(reply[2], err)
				batch.Amount, _ = redigo.Int64(reply[3], err)
				batch.Issued, _ = redigo.Bool(reply[4], err)
			}
			if batch.Issued {
				fmt.Printf("error: batch.Id %d already issued\n", batch.Id)
				resp.WriteHeader(200)
				return
			}

			userId_list, amount_list := splitAmount(batch.Amount, UserLen)
			for userInd := range userId_list {
				var OwnLen int64
				OwnLen, err = redigo.Int64(redis.Do("LLEN", "Own"))
				if err != nil {
					fmt.Println("err", err)
				}
				own := Own{
					Id:           OwnLen,
					UserId:       userId_list[userInd],
					BatchId:      batch.Id,
					Amount:       amount_list[userInd],
					AcquiredTime: time.Now().Unix(),
				}
				redis.Do("rpush", "Own", own.Id)
				redis.Do("hmset", "Own:"+strconv.Itoa(int(own.Id)), "id", own.Id, "userId", own.UserId, "batchId", own.BatchId, "amount", own.Amount, "acquiredTime", own.AcquiredTime)
			}
			redis.Do("hmset", "Batch:"+batchId, "issued", 1)

			resp.WriteHeader(200)
		}

	})
	sm.HandleFunc("/users", func(resp http.ResponseWriter, req *http.Request) {
		if req.Method == "GET" {
			// 查询用户

			redis := redisClient.Get()
			defer redis.Close()

			reply, err := redigo.Int64s(redis.Do("sort", "Own", "get", "Own:*->id", "get", "Own:*->userId", "get", "Own:*->batchId", "get", "Own:*->amount", "get", "Own:*->acquiredTime"))
			if err != nil {
				fmt.Println("err", err)
			}
			const _itemLen = 5
			OwnLen := len(reply) / _itemLen
			Own_list := make([]Own, OwnLen)
			for ownInd := 0; ownInd < OwnLen; ownInd++ {
				Own_list[ownInd] = Own{
					Id:           reply[ownInd*_itemLen+0],
					UserId:       reply[ownInd*_itemLen+1],
					BatchId:      reply[ownInd*_itemLen+2],
					Amount:       reply[ownInd*_itemLen+3],
					AcquiredTime: reply[ownInd*_itemLen+4],
				}
			}

			userId2UserResp := make(map[int64]*UserResp)

			for _, own := range Own_list {

				userResp_ptr, ok := userId2UserResp[own.UserId]
				if !ok {
					reply, err := redigo.Values((redis.Do("hmget", "User:"+strconv.Itoa(int(own.UserId)), "id", "nickname", "phoneNumber")))
					user := User{}
					{
						user.Id, _ = redigo.Int64(reply[0], err)
						user.Nickname, _ = redigo.String(reply[1], err)
						user.PhoneNumber, _ = redigo.String(reply[2], err)
					}
					userResp_ptr = &UserResp{
						User:             user,
						CouponStatistics: make([]CouponStatistic, 0, 10),
					}
					userId2UserResp[own.UserId] = userResp_ptr
				}
				reply, err := redigo.Values(redis.Do("hmget", "Batch:"+strconv.Itoa(int(own.BatchId)), "id", "name", "value", "amout", "issued"))
				if err != nil {
					fmt.Println("err", err)
				}
				batch := Batch{}
				{
					batch.Id, _ = redigo.Int64(reply[0], err)
					batch.Name, _ = redigo.String(reply[1], err)
					batch.Value, _ = redigo.Int64(reply[2], err)
					batch.Amount, _ = redigo.Int64(reply[3], err)
					batch.Issued, _ = redigo.Bool(reply[4], err)
				}
				couponStatistic := CouponStatistic{
					Name:         batch.Name,
					Value:        batch.Value,
					Amount:       own.Amount,
					AcquiredTime: own.AcquiredTime,
				}
				userResp_ptr.CouponStatistics = append(userResp_ptr.CouponStatistics, couponStatistic)
			}

			UserRespLen := len(userId2UserResp)

			UserResp_list := make([]UserResp, 0, UserRespLen)

			for _, v := range userId2UserResp {
				UserResp_list = append(UserResp_list, *v)
			}

			userSucResp := UserSucResp{
				Success: true,
				Data:    UserResp_list,
			}
			jsonData, _ := json.Marshal(userSucResp)
			resp.Write(jsonData)
		} else if req.Method == "POST" {

			redis := redisClient.Get()
			defer redis.Close()

			bodyData, err := ioutil.ReadAll(req.Body)
			if err != nil {
				fmt.Println("err", err)
			}
			var user User
			err = json.Unmarshal(bodyData, &user)
			if err != nil {
				fmt.Println("err", err)
			}

			UserLen, err := redigo.Int64(redis.Do("LLEN", "User"))
			if err != nil {
				fmt.Println("err", err)
			}

			user.Id = UserLen
			redis.Do("rpush", "User", user.Id)
			redis.Do("hmset", "User:"+strconv.Itoa(int(user.Id)), "id", user.Id, "nickname", user.Nickname, "phoneNumber", user.PhoneNumber)
			resp.WriteHeader(200)

		}

	})

	sm.HandleFunc("/test", func(resp http.ResponseWriter, req *http.Request) {
		// resp.Header().Set("Access-Control-Allow-Origin", "*")
		// resp.Header().Add("Access-Control-Allow-Headers", "Content-Type") //header的类型
		// resp.Header().Set("content-type", "application/json")             //返回数据格式是json

		// JSONObj none
		type JSONObj struct {
			Id      int64  `json:"id"`
			Name    string `json:"name"`
			Message string `json:"message"`
		}
		jsonObj := &JSONObj{
			Id:      1,
			Name:    "bencq",
			Message: "this message implies the successful call of the test api",
		}
		fmt.Println(jsonObj)

		data, err := json.Marshal(jsonObj)
		if err != nil {
			fmt.Println("err", err)

		}
		resp.Write(data)
		resp.WriteHeader(200)

	})
	// handler := cors.AllowAll().Handler(sm)
	handler := sm
	http.ListenAndServe(":8080", handler)
}

func initRedigoConn() error {
	redisHost := "127.0.0.1"
	redisPort := "6379"
	redisClient = &redigo.Pool{
		MaxIdle:     1,
		MaxActive:   10,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", redisHost+":"+redisPort)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
	return nil
}

func main() {
	rand.Seed(time.Now().Unix())
	if err := initRedigoConn(); err != nil {
		fmt.Println("err", err)
		return
	}
	// defer redis.Close()
	fmt.Println("dialed successfully")
	initServerMux()

}

// sm.HandleFunc("/users", func(resp http.ResponseWriter, req *http.Request) {
// 	if req.Method == "GET" {
// 		// 查询用户
// 		fmt.Println(req.RequestURI)
// 		// strings.Split() req.RequestURI
// 		reply, err := redigo.Strings(redis.Do("sort", "User", "get", "User:*->id", "get", "User:*->nickname", "get", "User:*->phoneNumber"))
// 		if err != nil {
// 			fmt.Println("err", err)
// 		}
// 		const _itemLen = 3
// 		UserLen := len(reply) / _itemLen
// 		user_list := make([]User, UserLen)
// 		for userInd := 0; userInd < UserLen; userInd++ {
// 			id, _ := strconv.ParseInt(reply[userInd*_itemLen+0], 0, 0)
// 			nickname := reply[userInd*_itemLen+1]
// 			phoneNumber := reply[userInd*_itemLen+1]
// 			user_list[userInd] = User{
// 				Id:          id,
// 				Nickname:    nickname,
// 				PhoneNumber: phoneNumber,
// 			}
// 		}
// 		userSucResp := UserSucResp{
// 			Success: true,
// 			Data:    user_list,
// 		}
// 		jsonData, _ := json.Marshal(userSucResp)
// 		resp.Write(jsonData)
// 	}

// })
