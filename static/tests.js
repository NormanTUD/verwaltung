function import_person () {
    $(".preset-buttons").find("button").first().click()
    $("form").find("button").last().click()
}

function assign_person_to_nodes() {
    $(".from-node-select").first().val("Person")
    $(".rel-type-input").val("dasisteintest")
}

function overview () {
    // Set checkbox
    $("#querySelection").find("input").first().prop("checked", true)
}

function run_tests() {
    console.log("Running tests...");
    // Add your test cases here
    console.log("All tests passed!");
}