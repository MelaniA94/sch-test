// Copyright (c) 2023, WSO2 LLC. (http://www.wso2.com). All Rights Reserved.
// 
// This software is the property of WSO2 LLC. and its suppliers, if any.
// Dissemination of any information or reproduction of any material contained
// herein in any form is strictly forbidden, unless permitted by WSO2 expressly.
// You may not alter or remove any copyright or other notice from copies of this content.

import ballerina/constraint;
import ballerina/time;

# Exact time (in UTC) and day of a week upto minute precision.
type TimeAndDayOfWeek record {|
    # Hour of the day (UTC)
    @constraint:Int {
        minValue: 0,
        maxValue: 23
    }
    int hour;
    # Minute of the day (UTC)
    @constraint:Int {
        minValue: 0,
        maxValue: 59
    }
    int minute;
    # Day of the week
    time:DayOfWeek[] daysOfWeek;
|};

# Configuration for a scheduled task triggered via an HTTP endpoint.
type ScheduledTask record {|
    # Task name
    string name;
    # Task trigger endpoint
    @constraint:String {
        pattern: {
            value: re `^http[s]?://.*`,
            message: "Endpoint should be a valid HTTP URL"
        }
    }
    string endpoint;
    # Scheduled period  
    # - `decimal` (in seconds): Scheduled to trigger after passing the set amount of time
    # - `TimeAndDayOfWeek`: Scheduled to trigger at the exact time and date of the week
    decimal|TimeAndDayOfWeek schedule;
|};

# Scheduled task log.
type ScheduledTaskLog record {|
    # Task name
    string name;
    # Last successfully triggered time (in ISO 8601 format)
    @constraint:String {
        pattern: re `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{2,3}Z$`
    }
    string lastTriggeredTime;
|};
