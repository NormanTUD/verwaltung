function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function import_person() {
    $(".preset-buttons").find("button").first().click()
    $("form").find("button").last().click()
}

async function assign_person_to_nodes_debug() {
    console.log("🚀 assign_person_to_nodes_debug() gestartet");

    try {
        // Helper für Sleep + Logging
        const sleep = (ms) => new Promise((resolve) => {
            console.log(`⏱️ Sleep für ${ms}ms...`);
            setTimeout(() => {
                console.log(`⏰ ${ms}ms vorbei`);
                resolve();
            }, ms);
        });

        // Check 1: jQuery verfügbar?
        if (typeof $ === "undefined") {
            console.error("❌ jQuery nicht gefunden!");
            return;
        } else {
            console.log("✅ jQuery erkannt, Version:", $.fn.jquery);
        }

        // Debug-Helfer
        const debugElement = (selector, name) => {
            const el = $(selector);
            if (el.length === 0) {
                console.error(`❌ ${name} (${selector}) nicht gefunden!`);
            } else {
                console.log(`✅ ${name} gefunden (${el.length}x):`, el);
            }
            return el;
        };

        console.group("🔍 Schritt 1: From-Node auswählen");
        const fromNode = debugElement(".from-node-select", "From-Node Select");
        fromNode.first().val("Person").trigger("change");
        console.log("👉 Wert gesetzt auf 'Person'");
        console.groupEnd();
        await sleep(150);

        console.group("🔍 Schritt 2: To-Node auswählen");
        const toNode = debugElement(".to-node-select", "To-Node Select");
        toNode.val("Stadt").trigger("change");
        console.log("👉 Wert gesetzt auf 'Stadt'");
        console.groupEnd();
        await sleep(150);

        console.group("🔍 Schritt 3: Relationship-Typ setzen");
        const relType = debugElement(".rel-type-input", "Relation Type Input");
        relType.val("dasisteintest").trigger("input").trigger("change");
        console.log("👉 Wert gesetzt auf 'dasisteintest'");
        console.groupEnd();
        await sleep(150);

        console.group("🔍 Schritt 4: Speichern-Button klicken");
        const saveButton = debugElement(".save-button", "Save-Button");
        console.log("🖱️ Klick wird ausgeführt...");
        saveButton.trigger("click");
        console.groupEnd();

        console.log("✅ Alle Schritte ausgeführt!");
    } catch (err) {
        console.error("💥 FEHLER in assign_person_to_nodes_debug:", err);
    }

    console.log("🏁 assign_person_to_nodes_debug() beendet");
}

function deactivate_checkbox(checkbox) {
    if (checkbox.prop("checked")) {
        checkbox.click();
    } else {
        log("Checkbox is already deactivated.");
    }
}

async function overview() {
    if(!$("#relationshipSelection").length) {
        error("Could not find relationship selection");
        return false;
    }
    if(!$("#relationshipSelection").find("input").length) {
        error("Could not find relationship selection inputs");
        return false;
    }

    if(!$("#relationshipSelection").find("input").first().is(":checked")) {
        error("First relationship is not checked");
        return false;
    }

    //$("#relationshipSelection").find("input").click()
    //await sleep(1000);

    if(!$("#querySelection").length) {
        error("Could not find query selection");
        return false;
    }

    if(!$("#querySelection").find("input").length) {
        error("Could not find query selection inputs");
        return false;
    }
    
    if($("#resultsContainer").text().trim() != "Ergebnisse") {
        error("Results container does not contain 'Ergebnisse'");
        return false;
    }

    $("#querySelection").find("input").first().click()
    await sleep(1000);

    if($("#resultsContainer").text().trim() == "Ergebnisse") {
        error("Results container does contain 'Ergebnisse'");
        return false;
    }

    deactivate_checkbox($("#querySelection").find("input").first())


    await sleep(1000);
    $("#querySelection").find("input").last().click()
    await sleep(1000);
    
    if($("#resultsContainer").text().trim() != "Stadt:stadt+Aktion+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen") {
        error("Results container does not contain 'Stadt'");
        return false;
    }

    await sleep(1000);
    $("#querySelection").find("input").first().click()
    await sleep(1000);

    if($("#resultsContainer").text().trim() != "Person:geburtsjahrPerson:nachnamePerson:titelPerson:vornameStadt:stadt+Aktion+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen") {
        error("Results container does not contain 'Person'");
        return false;
    }

    return true;
}

async function add_row_overview () {
    if(!$("#add_new_row").length) {
        error("Could not find add new row button");
        return false;
    }
    if ($(".query-results-table").find("tr").length != 11) {
        error("There are not 11 rows in the table");
        return false;
    }
    $("#add_new_row").click()
    await sleep (500)
    if ($(".query-results-table").find("tr").length != 12) {
        error("There are not 12 rows in the table after adding a row");
        return false;
    }

    return true;
}

async function wrote_overview () {
    if(!$(".query-results-table").length) {
        error("Could not find query results table");
        return false;
    }
    if ($(".query-results-table").find("tr").length != 12) {
        error("There are not 12 rows in the table");
        return false;
    }
    $(".query-results-table").find("tr").last().find("td").eq(0).find("input").val("Test").change()
    await sleep (500)
    if ($(".query-results-table").find("tr").last().find("td").eq(0).find("input").val() != "Test") {
        error("The value in the last row is not 'Test'");
        return false;
    }   
    return true;
}
async function delete_row_overview () {
    if(!$(".delete-btn").length) {
        error("Could not find delete button");
        return false;
    }
    if ($(".query-results-table").find("tr").length != 12) {
        error("There are not 12 rows in the table");
        return false;
    }
    await sleep (500)
    $(".delete-btn").last().click()
    await sleep (500)
    if ($(".query-results-table").find("tr").length != 11) {
        error("There are not 11 rows in the table after deleting a row");
        return false;
    }
}

async function define_rule ()  {
    if(!$(".form-control").length) {
        error("Could not find form controls");
        return false;
    }
    if (!$("#querybuilder_group_0").length) {
        error("Could not find query builder group");
        return false;
    }
    $(".form-control").first().val("Person.titel").change()
    if (!$(".form-controol").first().val() == "Person.titel") {
        error("The value of the first form control is not 'Person.titel'");
        return false;
    }
    await sleep (100)
    if (!$('.rule-container').first().find('.rule-operator-container select').length) {
        error("Could not find rule operator select");
        return false;
    }   
    $('.rule-container').first()
        .find('.rule-operator-container select')
        .val('equal')
        .trigger('change');
    if (!$('.rule-container').first().find('.rule-operator-container select').val() == 'equal') {
        error("The value of the rule operator select is not 'equal'");
        return false;
    }
    if (!$(".form-control").last().length) {
        error("Could not find last form control");
        return false;
    }
    await sleep (100)
    $(".form-control").last().val("Dr.").change()
    if (!$(".form-control").last().val() == "Dr.") {
        error("The value of the last form control is not 'Dr.'");
        return false;
    }
    
    return true;
}

async function add_new_rule() {
    if (!$(".btn-success").first().length) { 
        error("Could not find add new rule button");
        return false;
    }
    if ($("#querybuilder_rule_1").length) { 
        error("Rule 1 already exists");
        return false;
    }
    if (!$("#querybuilder_rule_0").length) { 
        error("Rule 0 does not exist");
        return false;
    }
    $(".btn-success").first().click()
    if (!$("#querybuilder_rule_1").length) { 
        error("Could not append more rules");
        return false;
    }

    return true;
}

async function save_rule() {
    if (!$("#queryNameInput").length) {
        error("Could not find query name input");
        return false;
    }
    if($("#queryNameInput").val().trim() !== "") {
        console.error("❌ Input ist nicht leer!");
        return false;
    }
    $("#queryNameInput").val("Testregel")
    await sleep (100)
    if ($("#queryNameInput").val() != "Testregel") {
        error("The value of the query name input is not 'Testregel'");
        return false;
    }
    if (!$("#save_overview_query").length) {
        error("Could not find save overview query button");
        return false;
    }
    $("#save_overview_query").click()
        if(input.val().trim() !== "") {
        console.error("❌ Fehler beim Speichern der Regel!");
        return false;
    }

    return true;
}

function delete_new_rule_overview() {
    if(!$(".btn-group").last().length) {
        error("Could not find delete new rule button");
        return false;
    }
    $(".btn-group").last().click()
    if($("#querybuilder_rule_1").length) {
        error("Could not delete new rule");
        return false;
    }

    return true;
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
    const elem = $(".block").first();
    if(!elem.length) {
        error("Could not find overview button");
        return false;
    }

    $(".block").first().click()

    return true;
}

function go_queries() {
    if(!$(".block").eq(1).length) {
        error("Could not find queries button");
        return false;
    }
    $(".block").eq(1).click()

    return true;
}

function go_admin_panel() {
    if(!$(".w-full").eq(3).length) {
        error("Could not find admin panel button");
        return false;
    }
    $(".w-full").eq(3).click()

    return true;
}

async function collection_import() {
    click_import()
    await sleep(500)
    import_person()
    await sleep(500)
    await assign_person_to_nodes()
}

async function collection_overview() {
    if(!go_overview()) {
        log("Could not go to overview");
        return false;
    }
    if(!await overview()) {
        log("Overview test failed");
        return false;
    }
    if(!await add_row_overview()) {
        log("Add row overview test failed");
        return false;
    } 
    if(!await wrote_overview()) {
        log("Wrote overview test failed");
        return false;
    }
    if(!await delete_row_overview()) {
        log("Delete row overview test failed");
        return false;
    }
    if(!await define_rule()) {
        log("Define rule test failed");
        return false;
    }
    if(!await save_rule()) {
        log("Save rule test failed");
        return false;
    }
    if(!await add_new_rule()) {
        log("Add new rule test failed");
        return false;
    }
    if(!delete_new_rule_overview()) {
        log("Delete new rule overview test failed");
        return false;
    }


    return true;
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