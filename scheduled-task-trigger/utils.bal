// Copyright (c) 2023, WSO2 LLC. (http://www.wso2.com). All Rights Reserved.
// 
// This software is the property of WSO2 LLC. and its suppliers, if any.
// Dissemination of any information or reproduction of any material contained
// herein in any form is strictly forbidden, unless permitted by WSO2 expressly.
// You may not alter or remove any copyright or other notice from copies of this content.

import ballerina/constraint;
import ballerina/file;
import ballerina/io;
import ballerina/log;
import ballerina/time;

configurable decimal choreoChronSchedule = 60d;
configurable string lastTriggerTimeLogFilepath = "/logs/lastSyncTime.log";

# Validate the given scheduled tasks and return the valid tasks.
# 
# + tasks - Scheduled tasks to be validated
# + return - Valid scheduled tasks
function getValidTasks(ScheduledTask[] tasks) returns ScheduledTask[] {
    ScheduledTask[] validTasks = [];
    foreach ScheduledTask task in tasks {
        if validTasks.some(validTask => validTask.name == task.name) {
            log:printWarn(string `Duplicate task: ${task.name}`);
            continue;
        }

        ScheduledTask|error validatedTask = constraint:validate(task);
        if validatedTask is error {
            log:printWarn(string `Invalid task: ${task.name}`, validatedTask);
            continue;
        }

        validTasks.push(validatedTask);
    }

    return validTasks;
}

# Check whether the scheduled task log is initialized.
# 
# + return - `()` on success
function initLog() returns error? {
    log:printDebug(string `Checking whether the last trigger time log exists.`);
    boolean isLogFileExists = check file:test(lastTriggerTimeLogFilepath, file:EXISTS);
    if !isLogFileExists {
        log:printInfo(string `File does not exist, creating the last trigger time log: ${lastTriggerTimeLogFilepath}.`);
        check io:fileWriteJson(lastTriggerTimeLogFilepath, []);
    }
}

# Check whether the scheduled task with the given name is ready to be triggered
# by comparing its last run time with the configured schedule period.
# 
# + task - Scheduled task to be checked
# + return - Whether the task is ready to be triggered
function isReadyForTrigger(ScheduledTask task) returns boolean|error {
    ScheduledTask {name: taskName, schedule} = task;

    log:printDebug(string `Reading the last trigger time log: ${lastTriggerTimeLogFilepath}.`);
    json logFile = check io:fileReadJson(lastTriggerTimeLogFilepath);
    ScheduledTaskLog[] logs = check logFile.fromJsonWithType();
    
    ScheduledTaskLog[] taskLog = logs.filter(log => log.name == task.name);
    if taskLog.length() == 0 {
        log:printDebug(string `Adding new log entry for the task: ${taskName}.`);
        logs.push({
            name: taskName,
            lastTriggeredTime: time:utcToString([0])
        });
        check io:fileWriteJson(lastTriggerTimeLogFilepath, logs.toJson());
        return true;
    }

    time:Utc lastTriggeredTime = check time:utcFromString(taskLog[0].lastTriggeredTime);
    boolean isReady = false;
    if schedule is decimal {
        isReady = time:utcDiffSeconds(time:utcNow(), lastTriggeredTime) > schedule;
    } else {
        time:Utc timeNowUtc = time:utcNow();
        time:Civil timeNow = time:utcToCivil(timeNowUtc);
        time:Civil scheduleTime = timeNow.clone();
        scheduleTime.hour = schedule.hour;
        scheduleTime.minute = schedule.minute;
        scheduleTime.second = 0d;
        time:Seconds currentToScheduleTimeDiff = time:utcDiffSeconds(
            check time:utcFromCivil(timeNow),
            check time:utcFromCivil(scheduleTime)
        );

        // Current time is matched with the configured time +/- 2 * Choreo cron period time
        boolean isCloseToExactTime = currentToScheduleTimeDiff.abs() < 2 * choreoChronSchedule;

        // Current day of the week is matched with the configured days of the week
        boolean isExactDayOfWeek = schedule.daysOfWeek.some(day => day == timeNow.dayOfWeek);

        boolean isLastTriggeredToday = time:utcToString(timeNowUtc).substring(0, 10) 
            == time:utcToString(lastTriggeredTime).substring(0, 10);

        isReady = isCloseToExactTime && isExactDayOfWeek && !isLastTriggeredToday;
    }

    log:printDebug(string `Task: ${taskName} is${isReady ? "" : " not"} ready to be triggered.`);
    return isReady;
}

# Update the last trigger time for the given scheduled task.
# 
# + task - Scheduled task to be updated
# + return - `()` on success
function updateTriggerTime(ScheduledTask task) returns error? {
    log:printDebug(string `Reading the last trigger time log: ${lastTriggerTimeLogFilepath}.`);
    json logFile = check io:fileReadJson(lastTriggerTimeLogFilepath);
    ScheduledTaskLog[] logs = check logFile.fromJsonWithType();

    foreach ScheduledTaskLog log in logs {
        if log.name == task.name {
            log.lastTriggeredTime = time:utcToString(time:utcNow());
        }
    }

    log:printDebug(string `Updating the last trigger time for task: ${task.name}.`);
    check io:fileWriteJson(lastTriggerTimeLogFilepath, logs.toJson());
}
