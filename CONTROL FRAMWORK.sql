



	-----ETL Control Framework ---

	--Control Environment
	--Control Frequency
	--Control PackageType
	--Control Package
	--Control Metrics
	---Control Anomalies----
	
	Use OsolaControl

	Create Table ctl.Envrinoment
	(
			EnvironmentID int,
			Environment nvarchar(255),
			Createdate datetime default getdate()
			constraint control_environment_Pk primary key (EnvironmentID)

	)

	insert into ctl.Envrinoment(EnvironmentID,Environment)
	Values
	(1,'Staging'),
	(2,'EDW')

	select * from ctl.Envrinoment


	select * from ctl.Envrinoment
	--Daiy, monthly ,weekly, Yearly

	Create Table ctl.Frequency
	(
		FrequencyID int,
		Frequency nvarchar(55),
		CreatedDate datetime default getdate(),
		constraint contril_frequency_pk primary key (FrequencyID)
	)
	
	insert into ctl.Frequency(FrequencyID,Frequency)
	Values
	(1,'Daily'),
	(2,'Weekly'),
	(3,'Monthly'),
	(4,'Yearly')


	
	Create Table ctl.PackageType
	(
		PackageTypeID int,
		PackageType nvarchar(55),
		CreatedDate datetime default getdate(),
		constraint control_PackageType_pk primary key (PackageTypeID)
	)
	
	insert into ctl.PackageType(PackageTypeID,PackageType)
	Values
	(1,'Dimension'),
	(2,'Fact')
	
	select * from ctl.Package
	drop table ctl.Package
	Create Table ctl.Package
	(
		PackageID int ,
		PackageName nvarchar(255),
		PackagetypeID int,
		SequenceNo int,
		EnvironmentID int,
		FrequencyID int,
		RunStartDate date,
		RunEndDate date,
		Active bit, --0 false, 1 True
		LastRunDate datetime,
		constraint control_package_pk primary key(packageID),
		Constraint control_package_PackagetypeID foreign key (PackagetypeID) references ctl.Packagetype(PackagetypeID),
		Constraint control_package_EnvironmentID foreign key (EnvironmentID) references ctl.Envrinoment(EnvironmentID),
		Constraint control_package_FrequencyID foreign key (FrequencyID) references ctl.Frequency(FrequencyID)
	)

	insert into ctl.Package(packageID,PackageName,PackagetypeID,SequenceNo,EnvironmentID,FrequencyID,RunStartDate,Active)
					Values 
                          (22,'Edw.Employee.dtsx',1,3300,2,1,convert(date,GETDATE()),1)
			
	  update  ctl.Package
	  set SequenceNo =  3100
	  where PackageID = 20

			select *
			from ctl.Package
			where PackageID = 11
		select * from ctl.Metrics		
		where RunDate = dateadd(day,-1,GETDATE())
					
					select GETDATE()

update ctl.package
set PackageName = 1500
where packageID =12

select * from ctl.Metrics

drop table Ctl.Metrics

	Create Table Ctl.Metrics
	(
	 MetricID bigint identity(1,1),
	 PackageID int,
	 StgSourceCount int,
	 StgDesCount int,
	 Precount int,
	 CurrentCount int,
	 Type1Count int,
	 Type2Count int,
	 PostCount int,
	 RunDate Datetime,
	 constraint control_Metrics_Pk primary key(MetricID),
	 Constraint control_PackageID_fk foreign key(PackageID) references ctl.package(PackageID)

	)

	select  * from ctl.Metrics

ALTER TABLE ctl.metrics drop CONSTRAINT control_PackageID_fk ;

	
	Alter table  ctl. Metrics
	add constraint control_PackageID_fk foreign key(PackageID) references ctl.package(PackageID)


	declare @PackageID int = ?
	declare @PreCount int =?
	declare @CurrentCount int = ?
	declare @Type1Count int = ?
	declare @Type2Count int = ?
	declare @PostCount int = ?

	insert into ctl.Metrics(PackageID,CurrentCount,Type1Count,Type2Count, PostCount,RunDate)
					Values(@PackageID,@PreCount,@CurrentCount,@Type1Count,@Type2Count,@PostCount,getdate())
					update ctl.Package set LastRunDate = GETDATE() where PackageID = @PackageID 
i	update ctl.Package set FrequencyID=3
	where PackageID = 4
	select * from ctl.Package
		

	select * from Ctl.Envrinoment
	select * from Ctl.Frequency
	select * from Ctl.PackageType 
`

update  Ctl.Package
set PackageName = 'stg.FactMisconductAnalysis.dtsx'
where PackageID = 14

alter table ctl.Package
alter column  PackageName PackageName

EXEC sp_rename 'ctl.Package.PackageName', 'PackageName';
	select * from Ctl.Package
	where convert(date,LastRunDate) <> convert(date, GETDATE())
	select 
	PackageID, PackageName, SequenceNo
	from 
	(--daily
    select PackageID,
	PackageName,
	SequenceNo,
	FrequencyID 
	from Ctl.Package
	where EnvironmentID = 1 and
	Active = 1 and
	FrequencyID = 1 and 
	RunStartDate  <=convert(date,GETDATE())
	 and
	RunEndDate is null or
	 RunEndDate <=  convert(date,GETDATE()) 
	 union all
	
	select PackageID,
	PackageName,
	SequenceNo,
	FrequencyID
	from Ctl.Package
	where EnvironmentID = 1 and
	Active = 1 and
	FrequencyID = 2 and 
	RunStartDate  <=convert(date,GETDATE())
	 and
	(RunEndDate is null or
	 RunEndDate <=  convert(date,GETDATE())) and
	datepart(weekday,dateadd(day,-1,getdate()))= 7
	union all
	select PackageID,
	PackageName,
	SequenceNo,
	FrequencyID
	from Ctl.Package
	where EnvironmentID = 1 and
	Active = 1 and
	FrequencyID = 3 and 
	RunStartDate  <=convert(date,GETDATE())
	 and
	(RunEndDate is null or
	 RunEndDate <=  convert(date,GETDATE())) and
	EOMONTH(dateadd(day,-1,convert(date, getdate()))) =  dateadd(day,-1,convert(date,getdate()))
	union all 
	select PackageID,
	PackageName,
	SequenceNo,
	FrequencyID
	from Ctl.Package
	where EnvironmentID = 1 and
	Active = 1 and
	FrequencyID = 4 and  
	RunStartDate  <=convert(date,GETDATE())
	 and
	(RunEndDate is null or
	 RunEndDate <=  convert(date,GETDATE())) and
	  EOMONTH(dateadd(day,-1, getdate())) =dateadd(day,-1,convert(date,getdate())) and  MONTH(dateadd(day,-1,GETDATE())) = 12 
	  ) t
		    order by  FrequencyID, SequenceNo 
	
	select dateadd(day,7,convert(date,getdate()))



	select MONTH(GETDATE())
	
	where packageID=11



	-- SSIS,  Talent,  Informatica Powercenter ,Pentaho


	run daily report