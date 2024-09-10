










<!--



ss_ua     = navigator.userAgent.toLowerCase();
ss_opera  = window.opera;
ss_msie   = (!ss_opera) && (ss_ua.indexOf("msie")          != -1);
ss_msie4  = (!ss_opera) && (ss_ua.indexOf("msie 4")        != -1);
ss_ns4    = (!ss_opera) && (ss_ua.indexOf("mozilla/4")     != -1) && (ss_ua.indexOf("compatible") == -1);
ss_ns6    = (!ss_opera) && (ss_ua.indexOf("netscape6/6.0") != -1);



ss_opera6lower = (ss_opera) && parseFloat(ss_ua.substr(ss_ua.indexOf("opera")+6)) < 7;
ss_no_dyna_script = ss_opera6lower;

ss_domain          = "ssif1.globalsign.com"
ss_fqdn            = "www.maliagroup.com"
ss_size            = "SZ100-40"
ss_type            = "image"
ss_lang            = "en"
ss_ver             = "V0024"
ss_service         = "S001"
ss_protocol        = "https"
ss_width           = ""
ss_height          = ""
ss_deter_dn          = ""
ss_imageLocation   = "//" + ss_domain + "/SiteSeal/siteSeal/siteSeal/siteSealImage.do?p1=" + ss_fqdn + "&p2=" + ss_size + "&p3=image" + "&p4=" + ss_lang + "&p5=" + ss_ver + "&p6=" + ss_service + "&p7=" + ss_protocol + "&deterDn=" + ss_deter_dn;
ss_flashLocation   = "//" + ss_domain + "/SiteSeal/siteSeal/siteSeal/siteSealImage.do?p1=" + ss_fqdn + "&p2=" + ss_size + "&p3=flash" + "&p4=" + ss_lang + "&p5=" + ss_ver + "&p6=" + ss_service + "&p7=" + ss_protocol + "&deterDn=" + ss_deter_dn;



ss_dimensions = ss_size.replace("SZ", "").split('-');


ss_msg = "";





function ss_open_sub(){
  ss_profile_domain  = "profile.globalsign.com"
  ss_url_p1          = "6b5fad40"
  ss_url_p2          = "5d2ad25871274489d264924d884261fe046b32630e76890d266219ce9372c016d75a1eb9432d30661b6e901f6d46468bd1688f17cf1a21"
  ss_url_p3          = "667c55407e4116374441fc116c54b4b939e40d21"
  ss_profileLocation = "https://" + ss_profile_domain + "/SiteSeal/siteSeal/profile/profile.do?p1=" + ss_url_p1 + "&p2=" + ss_url_p2 + "&p3=" + ss_url_p3;
  window.open(
    ss_profileLocation,
    'ss_wnd',
    'status=1,location=1,scrollbars=1,resizable=0,width=600,height=915'
  );
}







function ss_sealTagStr(){
  var str = "";
  if (ss_type == "image") {
    str = writeImage();
  } else {
    str = writeFlashImage();
  }
  return str;
}


function writeFlashImage() {
  var str = "";
  var flash = false;
  var expect_flash_version = 6;
  if ( navigator.mimeTypes && navigator.mimeTypes["application/x-shockwave-flash"] && navigator.mimeTypes["application/x-shockwave-flash"].enabledPlugin ) {
    var flash_version = 0;
    var flash_str = navigator.plugins["Shockwave Flash"].description.split(" ");
    for (var i = 0; i < flash_str.length; i++){
      if (isNaN(parseInt(flash_str[i])) == false) {
        flash_version = flash_str[i];
        break;
      }
    }
    flash = flash_version >= expect_flash_version;

  } else if (navigator.userAgent && navigator.userAgent.indexOf("MSIE") != -1 && (navigator.appVersion.indexOf("Win") != -1)) {
    document.write('<SC' + 'RIPT LANGUAGE=VBScript\> \n');
    document.write('on error resume next \n');
    
    document.write('display_flash = false \n');
    document.write('display_flash = ( IsObject(CreateObject("ShockwaveFlash.ShockwaveFlash.' + expect_flash_version + '"))) \n');
    document.write('</SC' + 'RIPT\> \n')
    flash = display_flash;
  }

  if ( flash ) {
    str = writeFlash();
  } else {
    if ( ss_service == "S001" ) {
      str = writeImage();
    } else {
      str = writeFlash();
    }

  }

  return str;
}


function writeFlash() {

  var str =
    "<OBJECT CLASSID='clsid:D27CDB6E-AE6D-11cf-96B8-444553540000'" +
    " CODEBASE='https://download.macromedia.com/pub/shockwave/cabs/flash/swflash.cab#version=6,0,0,0'" +
    " WIDTH='" + ss_width + "'" +
    " HEIGHT='" + ss_height + "'" +
    " NAME='ss_imgTag'>" +
    "<PARAM NAME=movie VALUE='" +  ss_flashLocation + "'>" +
    "<PARAM NAME='allowscriptaccess' VALUE='always'>" +
    "<PARAM NAME='quality' VALUE='best'>" +
    "<PARAM NAME='loop' VALUE='false'>" +
    "<PARAM NAME='menu' VALUE='false'>" +
    "<PARAM NAME='flashVars' VALUE='timeText=09:14:24(UTC)'>" +
    "<PARAM NAME='wmode' VALUE='transparent'>" +    
    "<EMBED src='" + ss_flashLocation + "' FlashVars='timeText=09:14:24(UTC)'" +
    " WIDTH='" + ss_width + "'" +
    " HEIGHT='" + ss_height + "'" +
    " NAME='ss_imgTag'" +
    " allowscriptaccess='always'" +
    " quality='best'" +
    " loop='false'" +
    " menu='false'" +
    " wmode='transparent'" +
    " TYPE='application/x-shockwave-flash'" +
    " PLUGINSPAGE='https://www.macromedia.com/go/getflashplayer'>" +
    "</EMBED>" +
    "</OBJECT>"
    ;

  return str;
}


function writeImage() {

  var str = "";
  var onmouse = "";

  if(ss_ns6 ){
    onmouse = "onmouseup='return ss_right(event);'";
  }

  str =
    
      "<img name='ss_imgTag' border='0' src='" + ss_imageLocation + "'" +
      " alt='" + ss_msg + "' oncontextmenu='return false;' galleryimg='no' style='width:" + ss_dimensions[0] + "px'>"
    
    ;

  return str;
}


function ss_seal(){

  
  if(ss_ns4 || ss_msie4){
    return;
  }


  
  var p8 = -1;
  try{
    if(window.top == window.self || window.top.location.host.toLowerCase() == window.self.location.host.toLowerCase()){
      p8 = 0;  
    }
    else {
      p8 = -2;
    }
  }
  catch(e1){
    p8 = -3;
  }

  
  if(p8 != 0){

    
    
    
    
      ss_imageLocation += "&p8=" + p8;
      ss_flashLocation += "&p8=" + p8;
    
  }


  
  var siteSeal = document.getElementById('ss_' + ss_size + '_' + ss_type + '_' + ss_lang + '_' + ss_ver + '_' + ss_service);

  
  if(siteSeal){
    siteSeal.setAttribute('id','ss_siteSeal_fin_' + ss_size + '_' + ss_type + '_' + ss_lang + '_' + ss_ver + '_' + ss_service);
  }

  
  if(siteSeal && siteSeal.innerHTML){
    siteSeal.innerHTML = "";
  }
  
  else{
    
    var imgtag;
    if (document.getElementById('___sitess__alt_img') != null) {
        imgtag = document.getElementById('___sitess__alt_img');
    } else {
        imgtag = document.getElementById('ss_img');
    }
    imgtag.width  = 0;
    imgtag.height = 0;
    imgtag.style.pixelWidth  = 0;
    imgtag.style.pixelHeight = 0;
    siteSeal.style.pixelWidth  = 0;
    siteSeal.style.pixelHeight = 0;

    
    imgtag.setAttribute('id','ss_img_fin_' + ss_size + '_' + ss_type + '_' + ss_lang + '_' + ss_ver + '_' + ss_service);
  }
  if (ss_type == "image"){
    
    if(ss_no_dyna_script) {
      document.write( ss_sealTagStr());
    }
    else{
      
      var ssTag = document.createElement("span");
      ssTag.innerHTML = ss_sealTagStr();
      siteSeal.parentNode.insertBefore(ssTag, siteSeal);
    }
  } else {
    document.write(ss_sealTagStr());
  }

}


function ss_right(e){
  if (e.which == 3) {
    return false;
  }
}


ss_seal();

//-->
