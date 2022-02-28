function collapse(level, msg, rowNum, cellNum) {
    /**
     * Collapse violation messages for a cell at given location.
     */
    // Remove the message from the value of the cell
    var value = document.getElementById(`value${rowNum}-${cellNum}`);
    value.innerText = value.innerText.replace(`\n${msg}`, "");

    // Add tooltip
    var td = document.getElementById(`td${rowNum}-${cellNum}`);
    td.setAttribute("data-bs-toggle", "tooltip");
    td.setAttribute("data-bs-placement", "bottom");
    td.setAttribute("data-bs-original-title", msg);
    td.setAttribute("title", msg);

    // Update btn
    var btn = document.getElementById(`expand${rowNum}-${cellNum}`);
    btn.href = `javascript:expand('${level}', '${msg}', ${rowNum}, ${cellNum})`;
    btn.innerHTML = '<i class="bi-plus"></i>';
}

function expand(level, msg, rowNum, cellNum) {
    /**
     * Expand violation messages for a cell at given location.
     */
    // Get the current cell value & add the message below
    var value = document.getElementById(`value${rowNum}-${cellNum}`);
    value.innerHTML = value.innerText + `<br><small class="fst-italic">${msg}</small>`;

    // Remove tooltip
    var td = document.getElementById(`td${rowNum}-${cellNum}`);
    var tooltipID = td.getAttribute("aria-describedby");
    td.removeAttribute("data-bs-toggle");
    td.removeAttribute("data-bs-placement");
    td.removeAttribute("data-bs-original-title");
    td.removeAttribute("title");
    td.removeAttribute("aria-describedby");
    var tt = document.getElementById(tooltipID);
    if (tt !== null) {
        tt.remove();
    }

    // Update btn
    var btn = document.getElementById(`expand${rowNum}-${cellNum}`);
    btn.href = `javascript:collapse('${level}', '${msg}', ${rowNum}, ${cellNum})`;
    btn.innerHTML = '<i class="bi-dash"></i>';
}

function reset() {
    /**
     * Reset the query parameters.
     */
    var url = window.location.href.split('?')[0];
    window.location.href = url;
}

function sort(col, desc) {
    /**
     * Sort query results. The actual sorting is done server-side,
     * but this bit of script creates the correct query parameters.
     */
    // Get current parameters
    var qString = window.location.search;
    var params = new URLSearchParams(qString);
    var newParams = [];
    var newOrder = [];
    // Add all current params to new params, only add order keys that are not col
    for (var entry of params.entries()) {
        if (entry[0] !== "order") {
            newParams.push(`${entry[0]}=${entry[1]}`);
        } else {
            for (var ord of entry[1].split(",")) {
                if (!ord.startsWith(col + ".") && ord !== col) {
                    newOrder.push(ord);
                }
            }
        }
    }
    // Add this col to the order
    if (desc) {
        newOrder.push(col + ".desc");
    } else {
        newOrder.push(col);
    }
    // Redirect to new URL
    newParams.push("order=" + newOrder.join(","));
    if (newParams.length > 0) {
        window.location.href = "?" + newParams.join("&");
    }
}

function submitQueryForm(headers, hidden) {
    /**
     * Submit the form to update query parameters and change search results. Include hidden form elements.
     */
    var args = []
    // Get the where options
    for (var h of headers) {
        var operator = document.getElementById(h + 'Operator').value;
        var constraint = document.getElementById(h + 'Constraint').value;
        if (operator && constraint) {
            args.push(h + "=" + operator + "." + constraint);
        }
    }

    // Get any hidden args
    for (var h of hidden) {
        var val = document.getElementById(h).value;
        args.push(h + "=" + val);
    }

    // Get the select options
    var n = document.getElementsByName('select[]');
    var s = [];
    for (i=0; i < (n.length); i++) {
        if (n[i].checked) {
            s.push(n[i].value);
        }
    }
    if (s.length > 0 && s.length < headers.length) {
        args.push("select=" + s.join(","));
    }

    // Get the violation filters
    var n = document.getElementsByName('violation[]');
    var v = [];
    for (i=0;i < (n.length); i++) {
        console.log(n[i].checked);
        if (n[i].checked) {
            v.push(n[i].value);
        }
    }
    if (v.length > 0) {
        args.push("violations=" + v.join(","));
    }

    // Get the limit option
    var l = document.getElementById("limitValue").value;
    args.push("limit=" + l);

    // Redirect to new URL
    if (args.length > 0) {
        window.location.href = "?" + args.join("&")
    }
}

function show_children() {
    /**
     * Redirect to sprocket table search results when clicking view all children.
     * This overrides gizmos default behavior.
     */
    var curURL = window.location.href;
    console.l
    var tableURL = curURL.substr(0, curURL.lastIndexOf("/"));
    var termId = curURL.substr(curURL.lastIndexOf("/") + 1);
    if (termId.includes("?")) {
        termId = termId.split("?")[0];
    }
    var url = new URL(tableURL);
    url.searchParams.append("subClassOf", termId);
    window.location = url;
}