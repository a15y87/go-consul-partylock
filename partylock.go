package consul_partylock

import "time"

type PartyLock struct {
	consulClient *consulLockClient
	LockTimeout  int
	Capacity     int
}

func New(TaskPath string, TaskWeight string, ConsulAddress string, LockTimeout int, Capacity int) (partyLock *PartyLock, err error) {

	var LockClient consulLockClient

	err = LockClient.Init(TaskPath, TaskWeight, ConsulAddress)
	if err != nil {
		return nil, err
	}
	partyLock = &PartyLock{&LockClient, LockTimeout, Capacity}

	return partyLock, err
}

func (s *PartyLock) Lock() (status bool, err error) {
	err = s.consulClient.AddWait()
	if err != nil {
		return false, err
	}

	time.Sleep(time.Duration(s.LockTimeout) * time.Millisecond)
	myPosition, err := s.consulClient.GetWaitPosition()
	if err != nil {
		return false, s.consulClient.DeleteWait()
	}
	if myPosition > (s.Capacity - 1) {
		return false, s.consulClient.DeleteWait()
	}

	locksCount, err := s.consulClient.GetLocksCount()
	if err != nil {
		return false, s.consulClient.DeleteWait()
	}

	err = s.consulClient.DeleteWait()
	if err != nil {
		return false, s.consulClient.DeleteWait()
	}

	if locksCount > (s.Capacity - 1) {
		return false, err
	}

	status, err = s.consulClient.AddLock()
	return status, err
}

func (s *PartyLock) UnLock() (status bool, err error) {
	time.Sleep(time.Duration(s.LockTimeout) * time.Millisecond)
	return s.consulClient.DeleteLock()
}
