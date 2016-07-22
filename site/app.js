var id = "";
var publicKey = "";
var pki = forge.pki;
var start = (new Date()).getTime();
var username = "";
var password = "";
var userid = "";
var tmpl = "";

function printTime() {
    var end = (new Date()).getTime();
    $("#result").append((end - start) / 1000 + "秒<br/>");
    start = (new Date()).getTime();
}

function getData(param) {
    $.ajax({
        url: "../submit?" + $.param(param),
        dataType: "json",
        success: function(data){
            id = data.id;
            printTime();
            if (data.status == "output_verifycode"){
                $('#captcha').empty();
                $('#captcha').html("");
                //没有验证码
                if (data.data.length == 0) {
                        $("#id_submit").style.display = "block";
                        return;
                }
                   
                $("#captcha").append("<img id=\"randcode_img\" src='" + data.data +"'/>");
                var changecaptcha = "<label for=\"randcode_img\" onclick=\"change_captcha()\"><span>\t\t刷新验证码</span></label>";
                $("#captcha").append(changecaptcha);
                var input = "<div class=\"form-group\">" +
                    "<label for=\"password\">验证码</label>" +
                    "<input type=\"txt\" class=\"form-control\" id=\"randcode_input\" placeholder=\"验证码\"></div>";
                $("#captcha").append(input);
                /*
                var button = "<div class=\"form-group\">"+
                    "<button type=\"submit\" onclick=\"sendRandcode();\" class=\"btn btn-default\" id=\"randcode_send\">发送验证码</button></div>";
                $("#result").append(button);
                 */
                 $("#id_submit").css('display','block');
            }
            else if(data.status == "need_param") {
                if (data.need_param == "password2"){
                    $("#result").empty();
                    $("#result").html("");
                    if (data.data.length != 0) {
                        $("#result").append("用户名："+data.data+"<br/>");
                    }
                    addPassword2Div("#result");
                }

                if (data.need_param == "phone") {
                    $("#result").empty();
                    $("#result").html("");
                    var phones = jQuery.parseJSON(data.data);

                    var phoneSelect = "<div class=\"form-group\"><select id=\"phone\">";
                    $.each(phones,function(index, value){
                        phoneSelect = phoneSelect + "<option value=\""+value+"\">"+value+"</option>"
                    });
                    phoneSelect = phoneSelect+"</select></div>";
                    $("#result").append(phoneSelect);

                    var button = "<div class=\"form-group\">"+
                        "<button type=\"submit\" onclick=\"sendPhone();\" class=\"btn btn-default\" id=\"randcode_send\">"+
                        "发送手机号码</button></div>";
                    $("#result").append(button);
                }

                /*
                if (data.need_param == "username") {
                    getData({id: data.id});
                }
                */
            } else if (data.status == "fail") {
                id = ""
                //if (tmpl == "taobao_shop" || tmpl == "tmall_shop"|| 
                if(tmpl == "10010" || tmpl == "pbccrc" ||
                    tmpl == "10086" || tmpl == "bjurbmi" ||
                    tmpl == "indinfo") {
                alert("抓取失败:"+data.data);
                $('#captcha').empty();
                $('#captcha').html("");
                $('#result').empty();
                $('#result').html("");
                $('#username').focus();
                getData({tmpl: tmpl});
            }
            } else if (data.status == "login_success") {
                $("#result").append("登录成功<br/>");
                getData({id: data.id});
            } else if (data.status == "login_fail") {
                $('#result').empty();
                $('#result').html("");
                $("#result").append("提示：" + data.data + "<br/>");
                getData({id: data.id});
            } else if (data.status == "begin_fetch_data") {
                $("#result").append("开始获取数据<br/>");
                getData({id: data.id});
            } else if (data.status == "finish_fetch_data") {
                $("#result").append("成功获取数据<br/>");
            } else if(data.status == "output_publickey") {
                publicKey = pki.publicKeyFromPem(data.data);
                id = data.id
                //getData({id: data.id, username: username, password: encrypted})
                getData({id: data.id})
                $("#result").append("获取公钥成功<br/>");
            }
        },
        error: function() {
        $("#result").append("超时");
            printTime();
        },
        timeout: 120000,
    });
}

function crawl() {
    username = $("#username").val();
    password = $("#password").val();
    userid = $("#userid").val();
    start = (new Date()).getTime();
    //tmpl = $("#tmpl").val();
    var randcode = "";

    if (id.length == 0) {
        alert("请刷新您的网页，重新登录！");
        return;
    }
    //是否需要验证码
    if ($("#randcode_input")) {
        randcode = $("#randcode_input").val();
        if (randcode.length ==0) {
            alert("验证码输入有误，请检查！");
            return;
        }
    }

    //if (username.length == 0 || password.length == 0 || tmpl.length == 0) {
    if (username.length == 0 || password.length == 0) {
        alert("您的输入有误，请检查！");
        return;
    }
    $("#result").html("开始<br/>");

    var encrypted = publicKey.encrypt(password, 'RSA-OAEP', {md: forge.md.sha256.create()});
    encrypted = forge.util.binary.hex.encode(encrypted);
    getData({id: id, username: username, password: encrypted, randcode:randcode})
    //getData({tmpl: tmpl,userid: userid});
}

function addPassword2Div(container) {
        var tip1 = "";
        var tip2 = "";

        if (tmpl == "10086"){
                tip1 = '短信验证码';
                tip2 = '提交短信验证码';
        }else if(tmpl == "pbccrc") {
                tip1 = '身份验证码';
                tip2 = '提交身份验证码';
        }else{
                tip1 = '独立密码';
                tip2 = '提交独立密码';
        }
        $(container).append('<div id="password2"></div>')
                $("#password2").append('<div class="form-group">'
                                + '<input type="password" class="form-control" id="password2_input" placeholder="' + tip1 + '" />'
                                + '</div>');
        $("#password2").append('<div class="form-group">'
                        + '<button type="submit" onclick="sendPassword2();" class="btn btn-default">'+ tip2 +'</button>'
                        + '</div>');


}

function sendPassword2() {
    var password2 = $("#password2_input").val();
    if (id.length == 0 || password2.length == 0) {
        alert("您的输入有误，请检查！");
        return;
    }

    getData({id: id, password2: password2});
}

function sendPhone() {
    alert("start send phone");
    var phone = $("#phone").val();
    if (id.length == 0 || phone.length == 0) {
        alert("您的输入有误，请检查！");
        return;
    }
    getData({id: id, phone: phone});
}

function sendRandcode() {
    var randcode = $("#randcode_input").val();
    if (id.length == 0 || randcode.length ==0) {
        alert("您的输入有误，请检查！");
        return;
    }
    getData({id: id, randcode: randcode});
}


/*
 * 
 * 2016-07-21
 */

function tippassword(tmpl){
        if (tmpl == "10010"){
            $("#labal_password").html("密码" + "<font color=\"red\">\t(温馨提示：忘记密码，发送 MMCZ#6位新密码 到10010！)</font>");
        }else if (tmpl == "10086"){
            $("#labal_password").html("密码" + "<font color=\"red\">\t(温馨提示：忘记密码，请前往移动官网获取密码！)</font>");
        }else if (tmpl == "pbccrc"){
            $("#labal_password").html("密码" + "<font color=\"red\">\t(温馨提示：忘记密码，请前往人行个人信用信息服务平台获取密码！)</font>");
        }else{
            $("#labal_password").html("密码");
        }
}

function getCaptcha() {
    $('#captcha').empty();
    $('#captcha').html("");
    tmpl = $("#tmpl").val();
    $("#tmpl option[value='']").remove();
    $('#result').empty();
    $('#result').html("");
    tippassword(tmpl);
    getData({tmpl: tmpl});
}

function change_captcha() {
    $('#result').empty();
    $('#result').html("");
    tmpl = $("#tmpl").val();
    getData({tmpl: tmpl});
}
