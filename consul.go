package consul_partylock

import (
	"github.com/hashicorp/consul/api"
	"sort"
	"strings"
)

type consulLockClient struct {
	taskPath           string
	taskWeight         string
	ConsulSessionID    string
	consulClient       *api.Client
	consulPathWait     string
	ConsulPathWaitTask string
	consulPathLock     string
	ConsulPathLockTask string
}

func (c *consulLockClient) Init(TaskPath, TaskWeight, ConsulAddress string) (err error) {
	c.taskPath = TaskPath
	c.taskWeight = TaskWeight
	se := &api.SessionEntry{}
	se.TTL = "10s"
	se.Behavior = "delete"

	ConsulConfig := api.DefaultConfig()
	ConsulConfig.Address = ConsulAddress

	c.consulClient, err = api.NewClient(ConsulConfig)
	if err != nil {
		return err
	}

	c.ConsulSessionID, _, err = c.consulClient.Session().Create(se, nil)
	if err != nil {
		return err
	}

	c.consulPathWait = c.taskPath + "/.wait/"
	c.consulPathLock = c.taskPath + "/.locked/"
	c.ConsulPathWaitTask = strings.Join([]string{c.consulPathWait, c.taskWeight, "-", c.ConsulSessionID}, "")
	c.ConsulPathLockTask = strings.Join([]string{c.consulPathLock, c.taskWeight, "-", c.ConsulSessionID}, "")

	return nil
}

func (c *consulLockClient) AddWait() (err error) {
	kv := c.consulClient.KV()
	p := &api.KVPair{Key: c.ConsulPathWaitTask, Value: []byte("0"), Session: c.ConsulSessionID}
	_, _, err = kv.Acquire(p, nil)
	return err
}

func (c *consulLockClient) DeleteWait() (err error) {
	kv := c.consulClient.KV()
	_, err = kv.Delete(c.ConsulPathWaitTask, nil)
	return err
}

func (c *consulLockClient) GetWaitPosition() (position int, err error) {
	kv := c.consulClient.KV()
	waits, _, err := kv.Keys(c.consulPathWait, "", nil)
	if err != nil {
		return 0, err
	}
	position = sort.SearchStrings(waits, c.ConsulPathWaitTask)
	return position, err
}

func (c *consulLockClient) RenewSession() (err error) {
	return c.consulClient.Session().Renew(c.ConsulSessionID, nil)
}


func (c *consulLockClient) AddLock() (status bool, err error) {
	kv := c.consulClient.KV()
	p := &api.KVPair{Key: c.ConsulPathLockTask, Value: []byte("0"), Session: c.ConsulSessionID}
	status, _, err = kv.Acquire(p, nil)
	return status, err
}

func (c *consulLockClient) DeleteLock() (status bool, err error) {
	kv := c.consulClient.KV()
	_, err = kv.Delete(c.ConsulPathLockTask, nil)
	if err != nil {
		return false, err
	}
	return true, err

}

func (c *consulLockClient) GetLocksCount() (count int, err error) {
	kv := c.consulClient.KV()
	locks, _, err := kv.Keys(c.consulPathLock, ";", nil)
	if err != nil {
		return 0, err
	}
	count = len(locks)
	return count, err
}
