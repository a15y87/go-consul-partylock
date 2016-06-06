package consul_partylock

import "time"

type PartyLock struct {
	ConsulClient *ConsulLockClient
	LockTimeout  int
	Capacity     int
}

func New(TaskPath string, TaskWeight string, ConsulAddress string, LockTimeout int) (PartyLock *PartyLock, err error) {
	PartyLock.LockTimeout = LockTimeout
	err = PartyLock.ConsulClient.Init(TaskPath, TaskWeight, ConsulAddress)
	return
}

func (s *PartyLock) Lock() (status bool, err error) {
	err = s.ConsulClient.AddWait()
	if err != nil {
		return false, err
	}

	time.Sleep(time.Duration(s.LockTimeout) * time.Millisecond)
	myPosition, err := s.ConsulClient.GetWaitPosition()
	if err != nil {
		return false, s.ConsulClient.DeleteWait()
	}
	if myPosition > (s.Capacity - 1) {

		return false, s.ConsulClient.DeleteWait()
	}

	locksCount, err := s.ConsulClient.GetLocksCount()
	if err != nil {
		return false, s.ConsulClient.DeleteWait()
	}

	err = s.ConsulClient.DeleteWait()
	if err != nil {
		return false, s.ConsulClient.DeleteWait()
	}

	if locksCount > (s.Capacity - 1) {
		return false, err
	}

	status, err = s.ConsulClient.AddLock()
	return status, err
}

func (s *PartyLock) UnLock() (status bool, err error) {
	time.Sleep(time.Duration(s.LockTimeout) * time.Millisecond)
	return s.ConsulClient.DeleteLock()
}

func (s *PartyLock) Destroy() (err error) {
	return s.ConsulClient.DestroySession()
}
