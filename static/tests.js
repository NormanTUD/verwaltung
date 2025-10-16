function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function import_person() {
    await sleep(500)
    if (!$(".preset-buttons").length) {
        error("Could not find preset buttons");
        return false;
    }
    $(".preset-buttons").find("button").first().click()
    await sleep(500)
    if (!$("#data").text() == " ") {
        error("Could not find data textarea");
        return false;
    }
    if (!$("form").length) {
        error("Could not find form");
        return false;
    }
    $("form").find("button").last().click()
    await sleep(500)
    return true;
}

async function assign_person_to_nodes() {
    console.log("🚀 assign_person_to_nodes() gestartet");

    try {
        // 🔍 Check: jQuery verfügbar?
        if (typeof $ === "undefined") {
            console.error("❌ jQuery nicht gefunden!");
            return false;
        }
        console.log("✅ jQuery erkannt, Version:", $.fn.jquery);

        // Hilfsfunktion für Element-Check
        const getEl = (selector, name) => {
            const el = $(selector);
            if (!el.length) {
                console.error(`❌ ${name} (${selector}) nicht gefunden!`);
                return null;
            }
            console.log(`✅ ${name} gefunden (${el.length}x)`);
            return el;
        };

        console.group("🔍 Schritt 1: From-Node auswählen");
        const fromNode = getEl(".from-node-select", "From-Node Select");
        if (!fromNode) return false;
        fromNode.first().val("Person").trigger("change");
        console.log("👉 Wert gesetzt auf 'Person'");
        console.groupEnd();
        await sleep(150);

        console.group("🔍 Schritt 2: To-Node auswählen");
        const toNode = getEl(".to-node-select", "To-Node Select");
        if (!toNode) return false;
        toNode.val("Stadt").trigger("change");
        console.log("👉 Wert gesetzt auf 'Stadt'");
        console.groupEnd();
        await sleep(150);

        console.group("🔍 Schritt 3: Relationship-Typ setzen");
        const relType = getEl(".rel-type-input", "Relation Type Input");
        if (!relType) return false;
        relType.val("dasisteintest").trigger("input").trigger("change");
        console.log("👉 Wert gesetzt auf 'dasisteintest'");
        console.groupEnd();
        await sleep(150);

        console.group("🔍 Schritt 4: Speichern-Button klicken");
        const saveButton = getEl(".save-button", "Save-Button");
        if (!saveButton) return false;
        console.log("🖱️ Klick wird ausgeführt...");
        saveButton.trigger("click");
        console.groupEnd();

        console.log("✅ Alle Schritte erfolgreich ausgeführt!");
        return true;

    } catch (err) {
        console.error("💥 FEHLER in assign_person_to_nodes:", err);
        return false;
    } finally {
        console.log("🏁 assign_person_to_nodes() beendet");
    }
}

function deactivate_checkbox(checkbox) {
    if (checkbox.prop("checked")) {
        checkbox.click();
    } else {
        log("Checkbox is already deactivated.");
    }

    
}

async function overview() {
    await sleep(2000)
    /*
    if (!$("#relationshipSelection").length) {
        error("Could not find relationship selection");
        return false;
    }
    */
    if (!$("#relationshipSelection").find("input").length) {
        error("Could not find relationship selection inputs");
        return false;
    }

    if (!$("#relationshipSelection").find("input").first().is(":checked")) {
        error("First relationship is not checked");
        return false;
    }

    //$("#relationshipSelection").find("input").click()
    //await sleep(1000);

    if (!$("#querySelection").length) {
        error("Could not find query selection");
        return false;
    }

    if (!$("#querySelection").find("input").length) {
        error("Could not find query selection inputs");
        return false;
    }

    if ($("#resultsContainer").text().trim() != "Ergebnisse") {
        error("Results container does not contain 'Ergebnisse'");
        return false;
    }

    $("#querySelection").find("input").first().click()
    await sleep(1000);

    if ($("#resultsContainer").text().trim() == "Ergebnisse") {
        error("Results container does contain 'Ergebnisse'");
        return false;
    }

    deactivate_checkbox($("#querySelection").find("input").first())


    await sleep(1000);
    $("#querySelection").find("input").last().click()
    await sleep(1000);

    if ($("#resultsContainer").text().trim() != "Stadt:stadt+Aktion+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen") {
        error("Results container does not contain 'Stadt'");
        return false;
    }

    await sleep(1000);
    $("#querySelection").find("input").first().click()
    await sleep(1000);

    if ($("#resultsContainer").text().trim() != "Person:geburtsjahrPerson:nachnamePerson:titelPerson:vornameStadt:stadt+Aktion+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen+Löschen") {
        error("Results container does not contain 'Person'");
        return false;
    }

    return true;
}

async function add_row_overview() {
    if (!$("#add_new_row").length) {
        error("Could not find add new row button");
        return false;
    }

    const old_number_trs = $(".query-results-table").find("tr").length;

    if (old_number_trs < 2) {
        error("There are less than 2 rows in the table");
        return false;
    }

    $("#add_new_row").click()
    await sleep(2000)
    
    if ($(".query-results-table").find("tr").length != (old_number_trs + 1)) {
        error("There are not 12 rows in the table after adding a row");
        return false;
    }

    return true;
}

async function wrote_overview() {
    if (!$(".query-results-table").length) {
        error("Could not find query results table");
        return false;
    }
    $(".query-results-table").find("tr").last().find("td").eq(0).find("input").val("Test").change()
    await sleep(500)
    if ($(".query-results-table").find("tr").last().find("td").eq(0).find("input").val() != "Test") {
        error("The value in the last row is not 'Test'");
        return false;
    }
    return true;
}
async function delete_row_overview() {
    if (!$(".delete-btn").length) {
        error("Could not find delete button");
        return false;
    }
    await sleep(500)

    const old_number_trs = $(".query-results-table").find("tr").length;
    if(old_number_trs < 2) {
        error("There are less than 2 rows in the table");
        return false;
    }
    
    $(".delete-btn").last().click()
    await sleep(100)

    if ($(".query-results-table").find("tr").length != (old_number_trs - 1)) {
        error(`There are not ${old_number_trs - 1} rows in the table after deleting a row`);
        return false;
    }

    return true;
}

async function define_rule() {
    if (!$(".form-control").length) {
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
    await sleep(100)
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
    await sleep(100)
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
    if ($("#queryNameInput").val().trim() !== "") {
        console.error("❌ Input ist nicht leer!");
        return false;
    }
    $("#queryNameInput").val("Testregel")
    await sleep(100)
    if ($("#queryNameInput").val() != "Testregel") {
        error("The value of the query name input is not 'Testregel'");
        return false;
    }
    if (!$("#save_overview_query").length) {
        error("Could not find save overview query button");
        return false;
    }
    $("#save_overview_query").click()

    await sleep(500)

    if ($("#queryNameInput").val().trim() !== "") {
        console.error("❌ Fehler beim Speichern der Regel!");
        return false;
    }

    return true;
}

function delete_new_rule_overview() {
    if (!$(".btn-group").last().length) {
        error("Could not find delete new rule button");
        return false;
    }
    $(".btn-group").last().click()
    if ($("#querybuilder_rule_1").length) {
        error("Could not delete new rule");
        return false;
    }

    return true;
}

async function rename_rule() {
    if (!$(".rename-btn").length) {
        error("Could not find rename button");
        return false;
    }
    name = $("#queryTableBody tr").first().find("td").first().text()
    $(".rename-btn").first().click()
    await sleep(100)
    if ($("#renameModal").find("input").val() != name) {
        error("The value in the rename modal is not the same as the rule name");
        return false;
    }
    if (!$("#renameModal").length) {
        error("Could not find rename modal");
        return false;
    }
    $("#renameModal").find("input").val("Umbenannt")
    await sleep(100)
    if ($("#renameModal").find("input").val() != "Umbenannt") {
        error("The value in the rename modal is not 'Umbenannt'");
        return false;
    }
    if (!$("#renameModal").find("button").last().length) {
        error("Could not find rename modal save button");
        return false;
    }
    $("#renameModal").find("button").last().click()

    await sleep(500)

    if ($("#queryTableBody tr").first().find("td").first().text() != "Umbenannt") {
        error("The rule name was not changed to 'Umbenannt'");
        return false;
    }
    return true;
}

async function delete_rule() {
    if (!$(".delete-btn").length) {
        error("Could not find delete button");
        return false;
    }
    $(".delete-btn").first().click()
    if (!$("#deleteModal").length) {
        error("Could not find delete modal");
        return false;
    }
    $("#deleteModal").find("button").last().click()

    await sleep(500)

    if ($("#queryTableBody tr").first().find("td").first().text() == "Umbenannt") {
        error("The rule was not deleted");
        return false;
    }
    return true;
}

async function add_user() {
    if (!$("#new_username").length) {
        error("Could not find new username input");
        return false;
    }
    if (!$("#new_username").val() == "") {
        error("New username input is not empty");
        return false;
    }
    $("#new_username").val("testuser")
    await sleep(100)
    if (!$("#new_username").val() == "testuser") {
        error("The value of the new username input is not 'testuser'");
        return false;
    }
    if (!$("#new_password").length) {
        error("Could not find new password input");
        return false;
    }
    $("#new_password").val("testuser")
    await sleep(100)
    if (!$("#new_role").length) {
        error("Could not find new role select");
        return false;
    }
    $("#new_role").val("1").change()
    await sleep(100)
    if (!$("#new_role").val() == "1") {
        error("The value of the new role select is not '1'");
        return false;
    }
    if (!$(".save-new").length) {
        error("Could not find save new user button");
        return false;
    }
    $(".save-new").last().click()
    await sleep(500)
    if ($(".user-entry").last().find("td").first().text() != "testuser") {
        error("The new user was not added");
        return false;
    }

    return true;
}

async function delete_user() {
    if (!$(".delete-entry").length) {
        error("Could not find delete user button");
        return false;
    }
    $(".delete-entry").last().click()
    if (!$("#confirmDelete").length) {
        error("Could not find confirm delete button");
        return false;
    }
    $("#confirmDelete").last().click()
    await sleep(500)
    if ($(".user-entry").length != 1) {
        error("The user was not deleted");
        return false;
    }

    return true;
}

function readonly_user() {
    if (!$("input[type='checkbox']").length) {
        error("Could not find readonly checkbox");
        return false;
    }
    $("input[type='checkbox']").last().click()
    if (!$("input[type='checkbox']").last().prop("checked")) {
        error("The readonly checkbox was not checked");
        return false;
    }
    return true;
}
async function activate_user() {
    if (!$(".save-new").first().length) {
        error("Could not find activate user button");
        return false;
    }
    $(".save-new").first().click()
    await sleep(500)
    if (!$(".activate-entry").first().hasClass("btn-success")) {
        error("The user was not activated");
        return false;
    }
    return true;
}

function go_import() {
    if (!$("#import_button").length) {
        error("Could not find import button");
        return false;
    }
    $("#import_button").click()

    return true;
}

async function go_overview() {
    const elem = $(".block").first();
    if (!elem.length) {
        error("Could not find overview button");
        return false;
    }

    $(".block").first().click()

    await sleep(500);

    return true;
}

async function go_index() {
    open_link("/")

    await sleep(500);

    return true;
}

async function go_queries() {
    if (!$(".block").eq(1).length) {
        error("Could not find queries button");
        return false;
    }
    $(".block").eq(1).click()

    await sleep(500);

    return true;
}

async function go_admin_panel() {
    if (!$(".w-full").eq(3).length) {
        error("Could not find admin panel button");
        return false;
    }
    $(".w-full").eq(3).click()

    await sleep(500);

    return true;
}
/*
async function login_test() {
    if (!$(".login-container").length) {
        error("Could not find login container");
        return false;
    }
    if (!$("#username").length) {
        error("Could not find username input");
        return false;
    }   
    $("#username").val("Admin1234/*")
    await sleep(100)
    if (!$("#password").length) {
        error("Could not find password input");
        return false;
    }
    $("#password").val("Admin1234/*")
    await sleep(100)
    if (!$(".save-new").length) {
        error("Could not find login button");
        return false;
    }
    $(".save-new").click()
    await sleep(500)
    return true;
}

async function go_login() {
    if (!$(".login-link").length) {
        error("Could not find login link");
        return false;
    }
    $(".login-link").find("a")[0].click()
    await sleep(500)
    return true;
}

async function go_register() {
    if (!$(".register-link").length) {
        error("Could not find register link");
        return false;
    }
    $(".register-link").find("a")[0].click()
    await sleep(500)
    return true;
}

async function register_test() {
    if (!$(".register-container").length) {
        error("Could not find register container");
        return false;
    }
    if (!$("#username").length) {
        error("Could not find username input");
        return false;
    }
    $("#username").val("Admin1234/*")
    await sleep(100)
    if (!$("#password").length) {
        error("Could not find password input");
        return false;
    }
    $("#password").val("Admin1234/*")
    await sleep(100)
        await sleep(100)
    if (!$(".save-new").length) {
        error("Could not find login button");
        return false;
    }
    $(".save-new").click()
    await sleep(500)
    if (!$(".save-new").length) {
        error("Could not find login button");
        return false;
    }
    $(".save-new").click()
    await sleep(500)
    return true;
}


async function login_works() {
    if (await login_test()) {
        log("Login test succeeded");
        return true;
    }
    if (!await go_register()) {
        log("Could not go to register");
        return false;
    }
    if (!await register_test()) {
        log("Register test failed");
        return false;
    }
    if (!await login_test()) {
        log("Login test failed");
        return false;
    }
    return true;
}


async function collection_start() {
    if (!$(".login-container").length) {
        if (!login_works()) {
            log("Login does not work");
            return true;
        }
    }
    if ($(".flex-grow").length) {
        log ("Already logged in");
        return true;
    }
    return true;
}

*/

async function collection_import() {
    if (!go_import()) {
        log("Could not go to import");
        return false;
    }
    if (!await import_person()) {
        log("Import person test failed");
        return false;
    }
    if (!await assign_person_to_nodes()) {
        log("Assign person to nodes test failed");
        return false;
    }
    return true;
}

async function collection_overview() {
    if (!await go_overview()) {
        log("Could not go to overview");
        return false;
    }
    
    if (!await overview()) {
        log("Overview test failed");
        return false;
    }
    if (!await add_row_overview()) {
        log("Add row overview test failed");
        return false;
    }
    if (!await wrote_overview()) {
        log("Wrote overview test failed");
        return false;
    }
    if (!await delete_row_overview()) {
        log("Delete row overview test failed");
        return false;
    }
    if (!await define_rule()) {
        log("Define rule test failed");
        return false;
    }
    if (!await save_rule()) {
        log("Save rule test failed");
        return false;
    }
    if (!await add_new_rule()) {
        log("Add new rule test failed");
        return false;
    }

    /*
    if (!delete_new_rule_overview()) {
        log("Delete new rule overview test failed");
        return false;
    }
    */

    return true;
}

async function collection_queries() {
    if (!await go_queries()) {
        log("Could not go to queries");
        return false;
    }
    if (!await rename_rule()) {
        log("Rename rule test failed");
        return false;
    }
    if (!await delete_rule()) {
        log("Delete rule test failed");
        return false;
    }

    return true;
}

async function collection_admin() {
    if (!await go_admin_panel()) {
        log("Could not go to admin panel");
        return false;
    }  
    if (!await add_user()) {
        log("Add user test failed");
        return false;
    }
    if (!await activate_user()) {
        log("Activate user test failed");
        return false;
    }
    if (!await readonly_user()) {
        log("Readonly user test failed");
        return false;
    }
    if (!await delete_user()) {
        log("Delete user test failed");
        return false;
    }
    return true;
}

async function delete_all_existing_queries() {
    if(!await go_queries()) {
        error("Could not go to queries");
        return false;
    }

    if (!$(".delete-btn").length) {
        log("No existing queries to delete");
        return true;
    } 

    while ($(".delete-btn").length) {
        $(".delete-btn").first().click()
        if (!$("#deleteModal").length) {
            error("Could not find delete modal");
            return false;
        }
        $("#deleteModal").find("button").last().click()
        await sleep(500)
    }

    if ($(".delete-btn").length) {
        error("Could not delete all existing queries");
        return false;
    }   

    return true;
}

async function delete_all() {
    if(!await delete_all_existing_queries()) {
        error("Could not delete all existing queries");
        return false;
    }

    if(!await go_index()) {
        error("Could not go to index");
        return false;
    }

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

function triggerLogout() {
    const logoutLink = $('a[href="/logout"]');
    if (!logoutLink.length) {
        console.error("❌ Logout-Link nicht gefunden!");
        return false;
    }

    console.log("🚪 Logout wird ausgeführt...");
    window.location.href = logoutLink.attr("href");
    if ($(".login-container").length) {
        console.log("✅ Logout erfolgreich!");
        return true;
    }
}

async function test_search() {
    if (!$("#sidebarSearch").length) {
        console.error("❌ Could not find search input");
        return false;
    }

    try {
        // 🧩 Schritt 1: Request mit await für 'admin'
        let adminData = await $.get('/search?q=admin');
        if (typeof adminData === 'string') {
            adminData = JSON.parse(adminData);
        }

        if (!Array.isArray(adminData)) {
            console.error('❌ Erwartet wurde ein Array für admin, erhalten:', typeof adminData);
            return false;
        }

        const hasAnyUrlAdmin = adminData.some(item => item.url);
        const hasAdminUrl = adminData.some(item => item.url === '/admin');

        if (!hasAnyUrlAdmin) {
            console.warn('⚠️ Keine URL in der Antwort für admin gefunden.');
            return false;
        }

        if (!hasAdminUrl) {
            console.warn('⚠️ URL "/admin" nicht gefunden.');
            return false;
        }

        console.log('✅ Admin-Test erfolgreich! /admin ist enthalten.');

        // 🧩 Schritt 2: Request für Autocomplete 'o'
        let autoData = await $.get('/search?q=o');
        if (typeof autoData === 'string') {
            autoData = JSON.parse(autoData);
        }

        if (!Array.isArray(autoData)) {
            console.error('❌ Erwartet wurde ein Array für Autocomplete, erhalten:', typeof autoData);
            return false;
        }

        // Prüfen ob Overview enthalten ist
        const hasOverview = autoData.some(item => item.url === '/overview');

        if (!hasOverview) {
            console.warn('⚠️ Autocomplete "/overview" nicht gefunden.');
            return false;
        }

        console.log('✅ Autocomplete-Test erfolgreich! "/overview" ist enthalten.');
        console.log('📦 Vollständige Autocomplete-Daten:', autoData);

        return true;

    } catch (err) {
        console.error('❌ Fehler beim Abruf von /search:', err);
        return false;
    }
}

async function queries_search_test() {
    if (!$("#sidebarSearch").length) {
        console.error("❌ Could not find search input");
        return false;
    }

    try {
        // 🧩 Schritt 1: Overview öffnen
        if (!await go_overview()) {
            console.error("❌ Could not go to overview");
            return false;
        }
        console.log("✅ go_overview erfolgreich");

        // 🧩 Schritt 2: Overview testen
        if (!await overview()) {
            console.error("❌ Overview test failed");
            return false;
        }
        console.log("✅ overview erfolgreich");

        // 🧩 Schritt 3: Regel definieren
        if (!await define_rule()) {
            console.error("❌ Define rule test failed");
            return false;
        }
        console.log("✅ define_rule erfolgreich");

        // 🧩 Schritt 4: Regel speichern
        if (!await save_rule()) {
            console.error("❌ Save rule test failed");
            return false;
        }
        console.log("✅ save_rule erfolgreich");

        // 🧩 Schritt 5: Search API testen nach "Testregel"
        let searchData = await $.get('/search?q=' + encodeURIComponent('Testregel'));
        if (typeof searchData === 'string') {
            try {
                searchData = JSON.parse(searchData);
            } catch (err) {
                console.error('❌ JSON Parsing der Search-Antwort fehlgeschlagen:', err);
                return false;
            }
        }

        if (!Array.isArray(searchData)) {
            console.error('❌ Erwartet wurde ein Array für Search, erhalten:', typeof searchData);
            return false;
        }

        const hasAnyUrl = searchData.some(item => item.url);
        const hasTestregel = searchData.some(item => item.label.includes('Testregel'));

        if (!hasAnyUrl) {
            console.warn('⚠️ Keine URLs in der Search-Antwort gefunden.');
            return false;
        }

        if (!hasTestregel) {
            console.warn('⚠️ Query "Testregel" wurde in den Suchergebnissen nicht gefunden.');
            return false;
        }

        console.log('✅ Search-Test erfolgreich! "Testregel" ist enthalten.');
        console.log('📦 Vollständige Search-Daten:', searchData);

        return true;

    } catch (err) {
        console.error('❌ Fehler beim Abruf von /search:', err);
        return false;
    }
}

async function search_schaefer_dresden_test() {
    if (!$("#sidebarSearch").length) {
        console.error("❌ Could not find search input");
        return false;
    }

    try {
        // --- Schritt 1: Suche nach "Schäfer" ---
        const term1 = 'Schäfer';
        $("#sidebarSearch").val(term1);
        console.log(`✅ Suchfeld mit "${term1}" gefüllt`);

        let data1 = await $.get('/search?q=' + encodeURIComponent(term1));
        if (typeof data1 === 'string') {
            try { data1 = JSON.parse(data1); } 
            catch (err) { console.error('❌ JSON Parsing fehlgeschlagen:', err); return false; }
        }

        if (!Array.isArray(data1)) {
            console.error('❌ Erwartet wurde ein Array für Search, erhalten:', typeof data1);
            return false;
        }

        const expectedUrl1 = '/overview?nodes=Person%2CStadt&relationships=DASISTEINTEST&qb=%7B%22condition%22%3A+%22AND%22%2C+%22rules%22%3A+%5B%7B%22id%22%3A+%22Person.nachname%22%2C+%22field%22%3A+%22Person.nachname%22%2C+%22type%22%3A+%22string%22%2C+%22input%22%3A+%22text%22%2C+%22operator%22%3A+%22equal%22%2C+%22value%22%3A+%22Sch%5Cu00e4fer%22%7D%5D%2C+%22valid%22%3A+true%7D';
        if (!data1.some(item => item.url === expectedUrl1)) {
            console.warn('⚠️ Erwartete URL für "Schäfer" nicht gefunden.');
            console.log('📦 Vollständige Search-Daten:', data1);
            return false;
        }
        console.log('✅ Suche nach "Schäfer" erfolgreich! URL korrekt.');

        // --- Schritt 2: Suche nach "Dresden" ---
        const term2 = 'Hannover';
        $("#sidebarSearch").val(term2);
        console.log(`✅ Suchfeld mit "${term2}" gefüllt`);

        let data2 = await $.get('/search?q=' + encodeURIComponent(term2));
        if (typeof data2 === 'string') {
            try { data2 = JSON.parse(data2); } 
            catch (err) { console.error('❌ JSON Parsing fehlgeschlagen:', err); return false; }
        }

        if (!Array.isArray(data2)) {
            console.error('❌ Erwartet wurde ein Array für Search, erhalten:', typeof data2);
            return false;
        }

        const expectedUrl2 = '/overview?nodes=Person%2CStadt&relationships=DASISTEINTEST&qb=%7B%22condition%22%3A+%22AND%22%2C+%22rules%22%3A+%5B%7B%22id%22%3A+%22Stadt.stadt%22%2C+%22field%22%3A+%22Stadt.stadt%22%2C+%22type%22%3A+%22string%22%2C+%22input%22%3A+%22text%22%2C+%22operator%22%3A+%22equal%22%2C+%22value%22%3A+%22Hannover%22%7D%5D%2C+%22valid%22%3A+true%7D';
        if (!data2.some(item => item.url === expectedUrl2)) {
            console.warn('⚠️ Erwartete URL für "Hannover" nicht gefunden.');
            console.log('📦 Vollständige Search-Daten:', data2);
            return false;
        }
        console.log('✅ Suche nach "Hannover" erfolgreich! URL korrekt.');

        return true;

    } catch (err) {
        console.error('❌ Fehler beim Abruf von /search:', err);
        return false;
    }
}





async function run_tests() {
    console.log("Running tests...");
    await delete_all();
    await sleep(1000);
    if (!await test_search()) {
        log("Search test failed");
        return false;
    }
    if (!await collection_import()) {
        log("Collection import test failed");
        return false;
    }
    if (!await collection_overview()) {
        log("Collection overview test failed");
        return false;
    }
    if (!await collection_queries()) {
        log("Collection queries test failed");
        return false;
    }
    if (!await queries_search_test()) {
        log("Queries search test failed");
        return false;
    }
    if (!await search_schaefer_dresden_test()) {
        log("Search Schäfer test failed");
        return false;
    }

    /*
    if (!await collection_admin()) {
        log("Collection admin test failed");
        return false;
    }
    */

    return true;
}
