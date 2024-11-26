from task_assembly.client import AssemblyClient
from task_assembly.cli import main
import unittest
import uuid


class TestBlueprint(unittest.TestCase):

    def setUp(self):
        self.client = AssemblyClient("")

    def tearDown(self):
        pass

    def test_blueprint(self):
        # create_blueprint
        before_allb = self.client.get_blueprints()
        new_blueprint = self.client.create_blueprint(
            name="blue_{0}".format(str(uuid.uuid4())[:4]), title="cli blueprint"
        )
        after_allb = self.client.get_blueprints()
        assert len(after_allb["blueprints"]) - len(before_allb["blueprints"]) == 1

        # get_blueprint
        get_blueprint = self.client.get_blueprint(id=new_blueprint["blueprintId"])
        assert get_blueprint["blueprintId"] == new_blueprint["blueprintId"]
        assert get_blueprint["title"] == "cli blueprint"

        blueprint_asset = self.client.create_blueprint_asset(
            blueprint_id=get_blueprint["blueprintId"], name="blueprint asset", kb=0
        )
        assert (
            blueprint_asset["created"]["attribute_values"]["blueprint_id"]
            == get_blueprint["blueprintId"]
        )


if __name__ == "__main__":
    unittest.main()
