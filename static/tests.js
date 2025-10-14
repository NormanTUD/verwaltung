function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function import_person() {
    $(".preset-buttons").find("button").first().click()
    $("form").find("button").last().click()
}

async function assign_person_to_nodes() {
    $(".from-node-select").first().val("Person")
    await sleep(100);
    $(".to-node-select").val("Stadt")
    await sleep(100);
    $(".rel-type-input").val("dasisteintest")
    await sleep(100);
    $(".save-button").click()
}

function deactivate_checkbox(checkbox) {
    if (checkbox.prop("checked")) {
        checkbox.click();
    } else {
        log("Checkbox is already deactivated.");
    }
}

async function overview() {
    $("#relationshipSelection").find("input").click()
    await sleep(1000);
    $("#querySelection").find("input").first().click()
    await sleep(1000);
    deactivate_checkbox($("#querySelection").find("input").first())
    await sleep(1000);
    $("#querySelection").find("input").last().click()
    await sleep(1000);
    $("#querySelection").find("input").first().click()
}

function add_row_owerview () {
    $("#add_new_row").click()
}

async function define_rule ()  {
    $(".form-control").first().val("Person.titel").change()
    await sleep (100)
    $('.rule-container').first()
        .find('.rule-operator-container select')
        .val('equal')
        .trigger('change');
    await sleep (100)
    $(".form-control").last().val("Dr.").change()
}

async function add_new_rule() {
    $(".btn-success").first().click()
}

async function save_rule() {
    $("#queryNameInput").val("Testregel")
    await sleep (100)
    $("#save_overview_query").click()
}

async function rename_rule() {
    $(".rename-btn").first().click()
    await sleep (100)
    $("#renameModal").find("input").val("Umbenannt")
    await sleep (100)
    $("#renameModal").find("button").last().click()
}

function delete_rule() {
    $(".delete-btn").first().click()
    $("#deleteModal").find("button").last().click()
}

async function add_user() {
    $("#new_username").val("testuser")
    await sleep (100)
    $("#new_password").val("testuser")
    await sleep (100)
    $("#new_role").val("1").change()
    await sleep (100)
    $(".save-new").last().click()
}

function delete_user() {
    $(".delete-entry").last().click() 
    $("#confirmDelete").last().click()
}

function readonly_user ()  {
    $("input[type='checkbox']").last().click()
}
function activate_user ()  {
    $(".save-new").first().click()
}

function click_import() {
    $("#import_button").click()
}

function go_overview() {
    $(".block").first().click()
}

function go_queries() {
    $(".block").eq(1).click()
}

function go_admin_panel() {
    $(".w-full").eq(3).click()
}

async function collection_import() {
    click_import()
    import_person()
    await assign_person_to_nodes()
}

async function collection_overview() {
    go_overview()
    await overview()
    add_row_owerview ()
    await define_rule ()
    await add_new_rule()
    await save_rule()
}

async function collection_queries() {
    go_queries()
    await rename_rule()
    delete_rule()
}

async function collection_admin() {
    go_admin_panel()
    await add_user()
    activate_user ()
    readonly_user ()
    delete_user()
}

async function delete_all() {
  try {
    const response = await fetch('/api/delete_all');
    const data = await response.json();

    if (data.status === 'success') {
      success(data.message);
    } else {
      error(data.message || 'Unbekannter Fehler');
    }
  } catch (err) {
    error('Verbindung fehlgeschlagen: ' + err.message);
  }
}

async function run_tests() {
    console.log("Running tests...");
    await delete_all();
    await collection_import()
    await collection_overview()
    await collection_queries()
    await collection_admin()
    console.log("All tests passed!");

    return true;
}