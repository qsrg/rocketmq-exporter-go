// License: Apache-2.0
package task
import ("context";"os";"testing";"time";"github.com/robfig/cron/v3";"github.com/wcf/rmq-exporter/internal/collector";"github.com/wcf/rmq-exporter/internal/service")
func TestLiveCronScheduling(t *testing.T){
	if os.Getenv("RMQ_LIVE_TESTS")!="1" { t.Skip("live") }
	ctx:=context.Background()
	admin:=service.NewAdminClient("127.0.0.1:9876",false,"","",10*time.Second)
	defer admin.Shutdown(ctx)
	coll:=collector.New(120*time.Second)
	pool:=New(10,5000); pool.Start(ctx); defer pool.Shutdown(ctx)
	ct:=&CollectTask{Admin:admin,Coll:coll,EnableCollect:true,Pool:pool}
	s:=cron.New(cron.WithSeconds())
	spec:="*/2 * * * * *"
	add:=func(fn func(context.Context)){ if _,err:=s.AddFunc(spec,func(){fn(ctx)});err!=nil{t.Fatal(err)} }
	add(ct.CollectTopicOffset); add(ct.CollectProducer); add(ct.CollectConsumerOffset)
	add(ct.CollectBrokerStatsTopic); add(ct.CollectBrokerStats); add(ct.CollectBrokerRuntimeStats)
	s.Start(); defer s.Stop()
	time.Sleep(8*time.Second)
	mfs,_:=coll.Gather()
	pop:=0; var names []string
	for _,f:=range mfs { if len(f.Metric)>0 { pop++; names=append(names,f.GetName()) } }
	t.Logf("cron-scheduled Gather: families=%d populated=%d names=%v",len(mfs),pop,names)
}
