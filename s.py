from task_assembly.client import AssemblyClient
from task_assembly.cli import main


if __name__ == '__main__':
    client = AssemblyClient("")
    #client.create_blueprint(name="hi", crowdconfig_service="service",crowdconfig_description="service_desc", task_template="task_template")
    #client.get_blueprints()
    #crowd_config = CrowdConfig.configure_crowd(service="service", title="title")
    #client.create_blueprint(name="hi", task_template="tasktemplatevalue", crowd_config=crowd_config)
    """
    client.create_blueprint(name="hi", state="state", title="title", description="description", keywords="keywords",
                            assignment_duration_seconds=0, lifetime_seconds=0, default_assignments=0,
                            max_assignments=0, default_team_id="1", template_uri="uri",
                            instructions_uri="uri", result_template_uri="uri", response_template_uri="uri")
    """
    main()
