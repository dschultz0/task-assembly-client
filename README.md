# Task Assembly Client
Tools for working with the Task Assembly service for managing crowdwork projects.

## Initial Setup
Start by installing the task-assembly client by running the following command.

```shell
pip install task-assembly
```

Note that the task-assembly client uses some AWS resources. If you haven't done so already, you will need to install
the AWS CLI and configure it with your credentials (`aws configure`) before using the client. 

Next, run the following command to configure your Task Assembly API key:

```shell
task-assembly configure --key <api-key>
```

At any point, you can run the following command to check that your account has been correctly configured:

```shell
task-assembly configure --validate
```

## Example project
If you are new to Task Assembly, you can start with a small sample project by running `task-assembly example` to 
copy a set of files to your working directory. These can be used with the instructions below to set up your first
project and help you build familiarity with how Task Assembly works.

## Project setup

First, we'll need to create a task-type for this project. This is just a placeholder and will be removed in
future updates to Task Assembly. Simply run the following command and capture the id that is generated.
When building the example application, use `NumbersExample` as the type name.

```shell
task-assembly create_task_type <type_name>
```

Now, we'll want to create a new Task Definition for our task using the Task Type created in the previous step.
When building the example application, use `Numbers` as the definition name.

```shell
task-assembly create_task_definition <definition_name> <task_type_id>
```

This will generate a new definition for our project and write a definition.json file that we will use to capture
our task attributes.

## Build the Task Definition
Now we can update the `definition.json` file with the appropriate attributes for our task. We've included
the core attributes for the example task below:

```yaml
DefinitionId: <your_definition_id>
TaskType: <your_task_type_id>
Title: Convert number to text
Description: Write out a number as text
RewardCents: 10
Lifetime: 3600
AssignmentDuration: 300
DefaultAssignments: 2
MaxAssignments: 5
AutoApprovalDelay: 0
```

You can now run the following command to update the definition in Task Assembly:

```shell
task-assembly update_task_definition
```

The task interface for our project is in the `template.html` file, and you will note that it uses a very similar 
templating language as Amazon SageMaker Ground Truth and the Amazon MTurk website. In most cases you can simply
copy/paste existing task templates from those tools and update the variable names within the `{{ name }}` values.
To apply this new task template to our definition, we can simply add a reference to it in our `definition.json` file:

```yaml
TemplateFile: template.html
```
Then run the `update_task_defintion` command again.
```shell
task-assembly update_task_definition
```

The `update_task_definition` command can be used to submit any future changes you make to your definition.json.

## Create a task in the sandbox
Now that we've completed the setup for our task, we should start by creating a test task in the *Sandbox* environment.
The Sandbox is a mirror of the Production environment but no money changes hands and work isn't generally completed
unless you do it yourself. This is a great way to validate that your task is set up correctly before putting it in the
hands of *real* Workers.

Note: The MTurk Sandbox is distinct from the Production environment, you will want to create a new account at
https://requestersandbox.mturk.com with the same email address as your Production account and also link it to the same
AWS account.

To create a single MTurk task in the Sandbox, you can use the following command. The `--sandbox` flag lets Task
Assembly know to create the task in the appropriate environment and the `--assignments 1` flag indicates that we
only need to ask one Worker to provide an answer to the task (just you). Finally, `number=4` provides the input
data to our task interface which refers to `{{ number }}` from the html template.

```shell
task-assembly create_task --sandbox --assignments 1 number=4
```

Take note of the task ID that is returned. You'll need that to retrieve the results.

This will create a new task for you in the Worker Sandbox, which you can view and work on by visiting
https://workersandbox.mturk.com. You'll likely need to create an account there, feel free to use your personal 
Amazon account or any other account you wish. Once you're logged in, you can search for your username or
the title of the task. Then you can accept and complete the task.

After you've completed your task, give MTurk and Task Assembly a few moments to process the result. Then run 
the following to get the output.

```shell
task-assembly get_task <task_id>
```

## Create a *real* task
Now we can repeat the same process to create a task in the Production environment by simply removing the
`--sandbox` and `--assignments` flags.

```shell
task-assembly create_task number=4
```

The same `get_task` command can be used to retrieve results of the created task.

## Result Processing
You may have noticed that the task result above includes `Result` and `Responses` values that looks a bit like this:

```json
{
  "Result": {
    "default": [
      {
        "WorkerId": "A12NBGVI9QN3DQ",
        "Result": {
          "default": {
            "numberAsText": "Four"
          }
        },
        "AssignmentId": "3ERMJ6L4DYRPC5L5VMTQ0TYQKQG7M2"
      },
      {
        "WorkerId": "ACKG8OU1KHKO2",
        "Result": {
          "default": {
            "numberAsText": "four"
          }
        },
        "AssignmentId": "3FTYUGLFSUK7L719U0FQJJX0K6T5D3"
      }
    ]
  },
  "Responses": [
    {
      "WorkerId": "A12NBGVI9QN3DQ",
      "Result": {
        "default": {
          "numberAsText": "Four"
        }
      },
      "AssignmentId": "3ERMJ6L4DYRPC5L5VMTQ0TYQKQG7M2"
    },
    {
      "WorkerId": "ACKG8OU1KHKO2",
      "Result": {
        "default": {
          "numberAsText": "four"
        }
      },
      "AssignmentId": "3FTYUGLFSUK7L719U0FQJJX0K6T5D3"
    }
  ]
}
```

This output is the `default` structure that Task Assembly uses and ensures that all the relevant data is included.
Of course, simplifying this output is preferred and will greatly simplify downstream data processing. To do this, we'll
use `handlers` that will perform data processing on each response and completed task. In most cases, AWS Lambda 
functions are the best option for these steps and Task Assembly helps this process by generating the Lambdas on your 
behalf if you wish. This tutorial will use Task Assembly to manage the Lambdas, but you're welcome to create and deploy
them normally using your preferred approach (Console, Cloud Formation, CDK, etc).

If building the example application you'll find the handler code for this tutorial in the `handlers.py` file. The first 
function `process_response` simply retrieves the `numberAsText` form field and returns it. We can pull it into our 
project adding the following to our `definition.json`:

```yaml
HandlerFile: handlers.py
SubmissionHandlers:
- Name:  value
  FunctionName:  process_response
```

This tells Task Assembly to run the `process_response` on the form outputs and return it in an attribute name `value`.
To test how this impacts task results, run the following to update the task definition and then *redrive* the task
results to reprocess the results:

```shell
task-assembly update_task_definition
task-assembly redrive_task <task-id>
```

Now the `Responses` list should be the following, simpler, representation of the Worker responses.

```json
[
  {
    "WorkerId": "A12NBGVI9QN3DQ",
    "Result": {
      "value": "Four"
    },
    "AssignmentId": "3ERMJ6L4DYRPC5L5VMTQ0TYQKQG7M2"
  },
  {
    "WorkerId": "ACKG8OU1KHKO2",
    "Result": {
      "value": "four"
    },
    "AssignmentId": "3FTYUGLFSUK7L719U0FQJJX0K6T5D3"
  }
]
```

Next, we want to consolidate these two responses into a single value as a result for this task. To do that, we'll 
add a *consolidation* handler. The `consolidate_result` function in the example handles this step by simply looking 
for agreement between Workers. If two Workers agree, then we'll use that result. If not, we'll return `{'extend': True}` 
which will prompt Task Assembly to ask an additional Worker to provide a response, up to the `MaxAssignments`. To add
this function to our task definition, we simply include the following in our `definition.json`.

```yaml
HandlerFile: handlers.py
SubmissionHandlers:
- Name:  value
  FunctionName:  process_response
ConsolidationHandlers: 
- Name: value
  FunctionName: consolidate_result
```

Running `update_task_definition` and `redrive_task` will invoke this new consolidation logic, and we will now have a
single value result for our task.

```json
{
  "value": "four"
}
```

## Create a batch
Now that our task is set up, we can run it on a larger set of data. If using the example, the `batch.csv` file contains 
a small batch of data. It is simply a single column CSV with column header `number` that corresponds to our task input, 
but multiple values can be used for more complex projects. To submit the batch, the following command can be used. The 
first parameter is the name we want to give the batch `numbers1`, followed by the source data file for the batch, 
`batch.csv`. Finally, we provide an S3 location we will use to store the input and output files created by Task Assembly.

```shell
task-assembly submit_batch numbers1 batch.csv s3://taskassembly-test/batches
```

The result is a batch_id we can use to monitor progress and retrieve the results. To get batch status, the following
can be used:

```shell
task-assembly get_batch_status <batch_id>
```

This command can be used to monitor your batch progress. When the batch is complete, running the following
to retrieve the results to a CSV file.

```shell
task-assembly get_batch_results <batch_id> output.csv
```

## Improving Quality
The last step is to establish some test tasks that we can use to ensure we're getting Workers who will 
do a good job on our task. For the example we've created a data file of known or *gold* answers in `gold.json`.
When Workers first start working on this task they will be prompted to answer at least two of these before
they can work on our *real* data.

To evaluate accuracy, we'll add a new handler `score_response` that scores each Worker's response. For
this we'll use a very simple comparison and give them a score of 100 if they provide an exact match,
80 if they don't match on the inclusion of "and" or "-" in their answer (i.e. "twenty-four" or 
"one hundred and six"), and 0 if they still don't match.

The following statements can be added to our `definition.json` file to enable scoring of new workers.
As you can see, Workers will need to complete at least two tests with an average score of 80 before 
they can begin working on the *real* tasks. Note that because it generally takes at least 10-15 seconds 
before scoring is complete so Workers will often be asked to do a third test if they quickly accept another
task after completing their second test.

```yaml
ScoringHandler: 
  FunctionName: score_response
GoldAnswersFile: gold.json
TestPolicy:
  MinTests: 2
  MinScore: 80
```

We can now create a new batch to begin using the new scoring that we've created. However, because scoring
doesn't work well with small batches, expand the size of your `batch.csv` file to at least 20 numbers
before submitting a new batch to Workers. This limitation is a result of how MTurk handles HITs with
fewer than 10 Assignments and will be addressed in a future release of Task Assembly.
Even with a larger set items in this test batch, you may not get responses for all of your items, however
subsequent batches should have higher yield.

Once you've submitted a new batch, you'll note that `get_batch_results` will now show test responses in
the results. Because this is the first time we've run a batch with scoring enabled, all Workers will need
to complete 2-3 test tasks. This means that we'll see a larger number of test responses and fewer *Task*
responses in our counts. In future batches the portion of responses associated with tests will steadily
decline.

```
 - Response Counts:
     Task: 36
     Test: 109
     Total: 145
```

To see how Workers did on your tests and their relative contribution to the final output, run
`task_assembly list_workers workers.csv` to generate a report on how many responses
each Worker has provided for this task definition and their scores on the test tasks.
