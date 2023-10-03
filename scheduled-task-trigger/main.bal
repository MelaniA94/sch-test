// Copyright (c) 2023, WSO2 LLC. (http://www.wso2.com). All Rights Reserved.
// 
// This software is the property of WSO2 LLC. and its suppliers, if any.
// Dissemination of any information or reproduction of any material contained
// herein in any form is strictly forbidden, unless permitted by WSO2 expressly.
// You may not alter or remove any copyright or other notice from copies of this content.

import ballerina/http;
import ballerina/log;

configurable ScheduledTask[] scheduledTasks = ?;

final readonly & int[] successStatusCodes = [http:STATUS_OK, http:STATUS_CREATED, http:STATUS_ACCEPTED];

# Trigger the scheduled tasks by invoking (POST) the configured endpoints.
# 
# + return - Returns an error if the task execution fails
public function main() returns error? {
    int successfullyTriggeredTasks = 0;

    check initLog();
    ScheduledTask[] validatedTasks = getValidTasks(scheduledTasks);

    log:printInfo("Starting the scheduler...");
    foreach ScheduledTask task in validatedTasks {
        log:printInfo(string `Triggering task: ${task.name}...`);
        if !(check isReadyForTrigger(task)) {
            log:printInfo(string `Task is not ready for triggering.`);
            continue;
        }

        http:Client|error taskClient = new (task.endpoint);
        if taskClient is error {
            log:printError(string `Error while creating the client for task: ${task.name}`, taskClient);
            continue;
        }

        http:Response|error response = taskClient->/.post(());
        if response is error {
            log:printError(string `Error while executing the task: ${task.name}`, response);
            continue;
        } else if successStatusCodes.indexOf(response.statusCode) is () {
            log:printError(string `Error while executing the task: ${task.name}`, statusCode = response.statusCode);
            continue;
        }

        check updateTriggerTime(task);
        successfullyTriggeredTasks += 1;
        log:printInfo("Successfully triggered the task.");
    }

    log:printInfo("Scheduler completed.", total = validatedTasks.length(), success = successfullyTriggeredTasks);
}
