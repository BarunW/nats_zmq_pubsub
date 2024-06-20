package models

import "time"

type Stream struct{
    StreamId string
    UserId int
    CameraId string 
    ProvenanceStreamId string           
    SourceServerId int
    DestinationServerId int
    ProcessId int
    StreamName string
    StreamUrl string 
    StreamType string
    Type string 
    IsPublic bool
    IsActive bool            
    IsPublishing bool            
    IsStable bool            
    TotalClients int           
    Codec string
    Resolution string            
    FrameRate uint
    BandwidthIn int64           
    BandwidthOut int64           
    BytesIn int64           
    BytesOut int64            
    ActiveTime uint            
    LastAccessed time.Time           
    LastActive time.Time         
    UpdatedAt time.Time
    CreatedAT time.Time
}
