package models

import "time"

 type Camera struct{
     CameraId string 
     UserId int
     ServerId int
     CameraName string
     CameraNum int
     CameraUsage string
     CameraOrientation string 
     City string 
     Junction string
     Location string
     UpdatedAt time.Time
     CreatedAT time.Time
}
