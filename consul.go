package consul_partylock

import (
	"github.com/hashicorp/consul/api"
	"sort"
	"strings"
)

type consulLockClient struct {
	taskPath           string
	taskWeight         string
	consulSession      string
	consulClient       *api.Client
	consulPathWait     string
	consulPathWaitTask string
	consulPathLock     string
	consulPathLockTask string
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

	c.consulSession, _, err = c.consulClient.Session().Create(se, nil)
	if err != nil {
		return err
	}

	c.consulPathWait = strings.Join([]string{c.taskPath, "/.wait/"}, "")
	c.consulPathLock = strings.Join([]string{c.taskPath, "/.locked/"}, "")
	c.consulPathWaitTask = strings.Join([]string{c.consulPathWaitTask, c.taskWeight, "-", c.consulSession}, "")
	c.consulPathLockTask = strings.Join([]string{c.consulPathLock, c.taskWeight, "-", c.consulSession}, "")

	return nil
}

func (c *consulLockClient) AddWait() (err error) {
	kv := c.consulClient.KV()
	p := &api.KVPair{Key: c.consulPathWaitTask, Value: []byte("0"), Session: c.consulSession}
	_, _, err = kv.Acquire(p, nil)
	return err
}

func (c *consulLockClient) DeleteWait() (err error) {
	kv := c.consulClient.KV()
	_, err = kv.Delete(c.consulPathWaitTask, nil)
	return err
}

func (c *consulLockClient) GetWaitPosition() (position int, err error) {
	kv := c.consulClient.KV()
	waits, _, err := kv.Keys(c.consulPathWait, ";", nil)
	if err != nil {
		return 0, err
	}
	position = sort.SearchStrings(waits, c.consulPathWaitTask)
	return position, err
}

func (c *consulLockClient) AddLock() (status bool, err error) {
	kv := c.consulClient.KV()
	p := &api.KVPair{Key: c.consulPathLockTask, Value: []byte("0"), Session: c.consulSession}
	status, _, err = kv.Acquire(p, nil)
	return status, err
}

func (c *consulLockClient) DeleteLock() (status bool, err error) {
	kv := c.consulClient.KV()
	_, err = kv.Delete(c.consulPathLockTask, nil)
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
