using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using System.Xml.XPath;

namespace HQCommon
{
    /// <summary> Implementations must provide a public default ctor, too. </summary>
    public interface IXmlPersistable
    {
        /// <summary> Stores this object to the given XmlElement (p_element)
        /// and returns p_element. (May return a different XmlElement as well,
        /// or null to indicate that this object shouldn't be saved.)
        /// p_context may be null.
        /// Precondition: the "class" attribute of p_element is set to the
        /// type name of this object (e.g. XMLUtils.SaveXmlPersistable()).
        /// </summary>
        XmlElement Save(XmlElement p_element, ISettings p_context);

        /// <summary> May be called once, after the default ctor. 
        /// p_context may be null. </summary>
        void Load(XmlElement p_element, ISettings p_context);
    }

    public static class XMLUtils
    {
        public static Dictionary<String, String> LoadDictionary(String p_filename)
        {
            using (StreamReader sr = File.OpenText(p_filename))
            {
                return ParseDictionary(sr);
            }
        }

        public static Dictionary<String, String> ParseDictionary(String p_xmlData)
        {
            StringReader sr = new StringReader(p_xmlData);
            return ParseDictionary(sr);
        }

        public static Dictionary<String, String> ParseDictionary(TextReader p_stream)
        {
            Dictionary<String, String> result = new Dictionary<string, string>();
            using (System.Xml.XmlTextReader xmlReader = new System.Xml.XmlTextReader(p_stream))
            {
                if (xmlReader.ReadToDescendant("properties"))
                {
                    String key = null;
                    while (xmlReader.Read())
                    {
                        XmlNodeType t = xmlReader.NodeType;
                        if (key == null) 
                        {
                            if (t == XmlNodeType.Element && xmlReader.Name == "property")
                                key = xmlReader.GetAttribute("name");
                        }
                        else if (t == XmlNodeType.Text || t == XmlNodeType.Whitespace)
                        {
                            result[key] = xmlReader.Value;
                            key = null;
                        }
                        else if (t == XmlNodeType.EndElement)
                        {
                            result[key] = "";
                            key = null;
                        }
                    }
                }
                xmlReader.Close();
            }
            return result;
        }

        /// <summary> Looks for &lt;property name=..>value&lt;/property> child nodes 
        /// in p_element and returns their contents. </summary>
        public static IEnumerable<KeyValuePair<string, string>> ParseDictionary(XmlElement p_element)
        {
            foreach (XmlElement elem in GetChildElements(p_element, "property"))
                yield return new KeyValuePair<string, string>(
                    elem.GetAttribute("name"), elem.InnerText);
        }

        /// <summary> Adds &lt;property name=..>value&lt;/property> child nodes 
        /// to p_container and returns p_container. </summary>
        public static XmlElement AppendDictionary<K,V>(XmlElement p_container, 
            IEnumerable<KeyValuePair<K, V>> p_dict)
        {
            if (p_dict == null)
                return p_container;
            XmlDocument doc = p_container.OwnerDocument;
            var invCult = System.Globalization.CultureInfo.InvariantCulture;
            foreach (var kv in p_dict)
            {
                XmlElement item = doc.CreateElement("property");
                item.SetAttribute("name", Convert.ToString(kv.Key, invCult));
                item.InnerText = Convert.ToString(kv.Value, invCult);
                p_container.AppendChild(item);
            }
            return p_container;
        }

        public static void SaveDictionary(IEnumerable<KeyValuePair<String, String>> p_dict, String p_filename)
        {
            using (StreamWriter sw = File.CreateText(p_filename))
            {
                WriteDictionary(p_dict, sw);
            }
        }

        public static string WriteDictionary(IEnumerable<KeyValuePair<String, String>> p_dict)
        {
            StringWriter sw = new StringWriter();
            WriteDictionary(p_dict, sw);
            return sw.ToString();
        }

        public static void WriteDictionary(IEnumerable<KeyValuePair<String, String>> p_dict, TextWriter p_stream)
        {
            using (System.Xml.XmlTextWriter xmlWriter = new System.Xml.XmlTextWriter(p_stream))
            {
                xmlWriter.Formatting = Formatting.Indented;
                xmlWriter.WriteStartDocument();
                xmlWriter.WriteStartElement("properties");
                foreach (KeyValuePair<String, String> kv in p_dict)
                {
                    xmlWriter.WriteStartElement("property");
                    xmlWriter.WriteAttributeString("name", kv.Key);
                    xmlWriter.WriteString(kv.Value);
                    xmlWriter.WriteEndElement();
                }
                xmlWriter.WriteEndElement();
                xmlWriter.WriteEndDocument();
                xmlWriter.Close();
            }
        }

        public static void SaveNode(this XmlNode p_node, string p_filename)
        {
            using (StreamWriter sw = File.CreateText(p_filename))
            {
                WriteNode(p_node, sw);
            }
        }

        public static string NodeToString(this XmlNode p_node)
        {
            StringWriter sw = new StringWriter();
            WriteNode(p_node, sw);
            return sw.ToString();
        }

        public static void WriteNode(XmlNode p_node, TextWriter p_stream)
        {
            if (p_node != null)
                using (XmlTextWriter xmlWriter = new XmlTextWriter(p_stream))
                {
                    xmlWriter.Formatting = Formatting.Indented;
                    xmlWriter.WriteStartDocument();
                    p_node.WriteTo(xmlWriter);
                    xmlWriter.WriteEndDocument();
                    xmlWriter.Close();
                }
        }

        public static XmlElement LoadNode(string p_filename)
        {
            using (StreamReader sr = File.OpenText(p_filename))
                return ParseNode(sr);
        }

        public static XmlElement ParseNode(string p_xmlData)
        {
            return ParseNode(p_xmlData, null, true);
        }

        /// <summary> Creates an XmlElement from the string in p_xmlData.
        /// p_doc specifies the target XmlDocument. If p_keepInDoc==true,
        /// p_doc.DocumentElement will contain the resulting XmlElement
        /// (faster) otherwise p_doc.DocumentElement is left unchanged.
        /// If p_doc is null or it's DocumentElement is already occupied,
        /// a temporary XmlDocument is created. Note that this is slow
        /// so should be avoided. </summary>
        public static XmlElement ParseNode(string p_xmlData, XmlDocument p_doc, bool p_keepInDoc)
        {
            if (String.IsNullOrEmpty(p_xmlData))
                return null;
            StringReader sr = new StringReader(p_xmlData);
            return ParseNode(sr, p_doc, p_keepInDoc);
        }

        public static XmlElement ParseNode(TextReader p_stream)
        {
            return ParseNode(p_stream, null, true);
        }

        public static XmlElement ParseNode(TextReader p_stream, XmlDocument p_doc, bool p_keepInDoc)
        {
            XmlDocument doc = p_doc;
            if (doc == null || (!p_keepInDoc && doc.DocumentElement != null))
                doc = new XmlDocument();
            using (p_stream)
            {
                doc.Load(p_stream);
                p_stream.Close();
            }
            XmlElement result = doc.DocumentElement;
            if (p_doc != null && !ReferenceEquals(doc, p_doc))
                result = (XmlElement)p_doc.ImportNode(result, true);
            else if (!p_keepInDoc)
                doc.RemoveAll();
            return result;
        }

        //[Obsolete("use GetChildElements() instead")]
        //public static IEnumerable<XmlElement> FindElements(XmlElement p_element, string p_name)
        //{
        //    return GetChildElements(p_element, p_name);
        //}

        /// <summary> Filters children of p_element for XmlElement type and p_name name </summary>
        public static IEnumerable<XmlElement> GetChildElements(this XmlElement p_element, string p_name)
        {
            if (p_element != null)
                foreach (XmlNode node in p_element.ChildNodes)
                {
                    XmlElement element = node as XmlElement;
                    if (element != null && element.Name == p_name)
                        yield return element;
                }
        }

        public static void RemoveAllChild(this XmlNode p_node)
        {
            if (p_node != null)
            {
                var it = p_node.ChildNodes.GetEnumerator();
                using (it as IDisposable)
                    if (it.MoveNext())
                    {
                        XmlNode prev = (XmlNode)it.Current;
                        for (; it.MoveNext(); prev = (XmlNode)it.Current)
                            p_node.RemoveChild(prev);
                        p_node.RemoveChild(prev);
                    }
            }
        }

        /// <summary> Returns a string that helps to locate p_node in the XML tree.
        /// Example: "Root[2]>Item>Child"  means that p_node.Name is "Child", it is
        /// the only child of "Item" which is the third child of the root.
        /// </summary>
        // TODO: make it compatible with XPath. E.g. "/Main/Item[3]/Child"
        // means that the root is named "Main", it may have many kind of children
        // but the third child of type <Item> has <Child> in it.
        public static string GetDebugPath(this XmlNode p_node)
        {
            var names = new List<string>();
            while (p_node != null)
            {
                string name = null;
                XmlNode parent = p_node.ParentNode;
                if (parent != null)
                {
                    XmlNodeList siblings = parent.ChildNodes;
                    int n = siblings.Count;
                    if (n > 1)
                        for (int i = n - 1; i >= 0; --i)
                            if (ReferenceEquals(siblings[i], p_node))
                            {
                                name = Utils.FormatInvCult("[{0}]>{1}", i, p_node.Name);
                                break;
                            }
                    if (name == null)
                        name = ">" + p_node.Name;
                }
                names.Add(name ?? p_node.Name);
                p_node = parent;
            }
            names.Reverse();
            return Utils.Join(String.Empty, names);
        }

        /// <summary>
        /// Example:<code> XMLUtils.Select&lt;int&gt;(navigator, "Settings/Portfolio/@ID"); </code><br/>
        /// Selects the values of the ID attribute from all Portfolio nodes under the Settings node
        /// and converts it to 'int' (using XPathNavigator.ValueAs()).
        /// To select whole elements instead of attributes, specify XmlElement as generic type argument
        /// (this will use XPathNavigator.UnderlyingObject). </summary>
        public static IEnumerable<T> Select<T>(XPathNavigator p_navigator, string p_xpath)
        {
            Type t = typeof(T);
            bool useValue = (typeof(string).Equals(t) || (!t.IsClass && !t.IsInterface));
            XPathNodeIterator nodeIterator = p_navigator.Select(p_xpath);
            if (useValue)
                while (nodeIterator.MoveNext())
                    yield return (T)nodeIterator.Current.ValueAs(t);
            else
                while (nodeIterator.MoveNext())
                    yield return (T)nodeIterator.Current.UnderlyingObject;
        }

        /// <summary> Calls Utils.TryParse() to convert from string to T (may be nullable type).
        /// Throws XmlException if the value of the attribute is present 
        /// and non-empty but cannot be parsed to type T. </summary>
		public static T GetAttribute<T>(XmlElement p_element, string p_attributeName, T p_defValue)
		{
			return GetAttribute<T>(p_element, p_attributeName, false, p_defValue, null);
		}

		private static T GetAttribute<T>(XmlElement p_element, string p_attributeName, 
            bool p_isRequiredAttribute, T p_defValue, Utils.Parser p_formatInfo)
        {
            if (p_element == null && !p_isRequiredAttribute)
                return p_defValue;
            if (p_element == null)
                throw new ArgumentNullException("p_element");
            XmlAttribute attr = p_element.GetAttributeNode(p_attributeName);
            if (attr == null)
            {
                if (p_isRequiredAttribute)
                    throw new XmlException(String.Format("{0}: missing '{1}' attribute",
                        p_element.GetDebugPath(), p_attributeName));
                return p_defValue;
            }
            Type t = typeof(T);
            if (typeof(String).Equals(t) || typeof(object).Equals(t))
                return (T)(object)attr.Value;
            KeyValuePair<Utils.ParseResult, T> kv = Utils.TryParse<T>(attr.Value, p_defValue, p_formatInfo);
            if (kv.Key == Utils.ParseResult.OK || (!p_isRequiredAttribute
                && kv.Key != Utils.ParseResult.Fail))
                return kv.Value;
            throw new XmlException(String.Format("{0}: invalid value for attribute {1}=\"{2}\"", 
                p_element.GetDebugPath(), p_attributeName, attr.Value));
        }
        /// <summary> Calls Utils.TryParse() to convert from string to T.
        /// Throws XmlException if p_attributeName is missing or its value is empty
        /// (T==string accepts empty value) </summary>
        /// <param name="p_formatInfo">see p_formatInfo parameter of Utils.TryParse&lt;T&gt;()</param>
        public static T GetRequiredAttribute<T>(this XmlElement p_element, string p_attributeName,
            Utils.Parser p_formatInfo = null)
        {
            return GetAttribute<T>(p_element, p_attributeName, true, default(T), p_formatInfo);
        }
        public static string GetRequiredAttribute(this XmlElement p_element, string p_attributeName)
        {
            return GetAttribute<string>(p_element, p_attributeName, true, null, null);
        }

        public static void SetAttributeIfNeq<T>(this XmlElement p_element, T p_comparand, 
            T p_value, string p_attributeName)
        {
            if (!EqualityComparer<T>.Default.Equals(p_value, p_comparand))
                SetAttribute<T>(p_element, p_attributeName, p_value);
        }
        public static void SetAttributeIf<T>(this XmlElement p_element, bool p_condition, 
            T p_value, string p_attributeName)
        {
            if (p_condition)
                SetAttribute<T>(p_element, p_attributeName, p_value);
        }
        private static void SetAttribute<T>(XmlElement p_element, string p_attrName, T p_value)
        {
            p_element.SetAttribute(p_attrName, Conversion<T, string>.Default.DefaultOnNull(p_value));
        }


        public static string ComposeTagNameFromType(Type p_type)
        {
            string name = p_type.Name;
            int i = name.IndexOf('`');
            return (i < 0) ? name : name.Substring(0, i);
        }

        /// <summary> Parses all public fields+properties of p_object from p_element.
        /// (Except those decorated with the [XmlIgnore] attribute.)<para>
        /// Parsing is done with Utils.Parser.TryParse(). For example, enums with [Flag]
        /// attribute are supported in either "const1, const2" or "const1|const2" formats.</para><para>
        /// If the [XmlNullReplacement] attribute is present, the value specified by
        /// that attribute is parsed to null.</para><para>
        /// Example:                                                  </para><para>
        ///    MyStruct x;                                            </para><para>
        ///    x = XMLUtils.ParsePublicProperties(x, element, null);  </para><para>
        /// <paramref name="p_formatInfo"/> may be an ISettings (context for LoadAnyType())
        /// or a Utils.Parser </para>
        /// </summary>
        public static T ParsePublicProperties<T>(T p_object, XmlElement p_element, object p_formatInfo = null)
        {
            if (p_element == null || (!p_element.HasAttributes && !p_element.HasChildNodes))
                return p_object;
            ISettings loadCtx = p_formatInfo as ISettings;
            var parser = p_formatInfo as Utils.Parser;
            object copy = p_object;
            foreach (MemberInfo m in XMLUtils.CacheOfFieldsAndProperties.Get(p_object.GetType()))
            {
                Type memberType = Utils.GetTypeOfMember(m);
                object value;
                if (typeof(IXmlPersistable).IsAssignableFrom(memberType)
                    || typeof(Delegate).IsAssignableFrom(memberType))
                    value = LoadAnyType(memberType, GetChildElements(p_element, m.Name)
                        .FirstOrDefault(), loadCtx);
                else
                {
                    value = null;
                    XmlAttribute input = p_element.Attributes[m.Name];
                    if (input != null && Utils.TryParse(input.Value, memberType, out value, 
                        parser) == Utils.ParseResult.Fail)
                        throw new XmlException(String.Format("{0}: invalid value {1}=\"{2}\"",
                            p_element.GetDebugPath(), m.Name, input.Value));
                }
                if (value == null)
                    continue;
                object[] attrs = m.GetCustomAttributes(typeof(XmlNullReplacementAttribute), true);
                if (0 < attrs.Length && Equals(((XmlNullReplacementAttribute)attrs[0]).Null, value))
                    value = null;
                Utils.SetValueOfMember(m, copy, value);
            }
            return (T)copy;
        }

        public class CacheOfFieldsAndProperties : StaticDict<Type, MemberInfo[], CacheOfFieldsAndProperties>
        {
            public override MemberInfo[] CalculateValue(Type p_type, object p_arg)
            {
                return PublicFieldsPropertiesStaticCache.Get(p_type).Where(m => !m.IsDefined(typeof(XmlIgnoreAttribute), true)).ToArray();
            }
        }

        /// <summary> Omits fields/properties having [XmlIgnore] attribute or 
        /// null value (except when there's an [XmlNullReplacement] attribute),
        /// or properties for which getter or setter does not exist or getter isn't public.
        /// p_element may be null (auto-created). p_context is used for values
        /// that implement IXmlPersistable; may be null. </summary>
        public static XmlElement SavePublicProperties(object p_object, XmlElement p_element, 
            ISettings p_context)
        {
            if (p_object == null)
                throw new ArgumentNullException();
            if (p_element == null)
                p_element = new XmlDocument().CreateElement(ComposeTagNameFromType(p_object.GetType()));

            var tmp = new FastGrowingList<object>();
            foreach (MemberInfo m in XMLUtils.CacheOfFieldsAndProperties.Get(p_object.GetType()))
            {
                MethodInfo getter;
                var p = m as PropertyInfo;
                if (p != null 
                    && (!p.CanWrite || null == (getter = p.GetGetMethod())  // both get & set must exist and get must be public
                        || getter.GetParameters().Length != 0))             // omit indexed properties
                    continue;
                object val = (p != null) ? p.GetValue(p_object, null) : ((FieldInfo)m).GetValue(p_object);
                object[] attrs;
                if (val == null && 0 < (attrs = m.GetCustomAttributes(
                    typeof(XmlNullReplacementAttribute), true)).Length)
                    val = ((XmlNullReplacementAttribute)attrs[0]).Null;
                if (val is Delegate)
                    val = DelegateXmlWrapper.CreateIfStaticDelegate(val);
                if (val != null)
                {
                    tmp.Add(m.Name);
                    tmp.Add(val);
                }
            }
            return (tmp.Count > 0) ? Extend(p_element, p_context, tmp.ToArray()) : p_element;
        }

        public static string PublicPropertiesToString(object p_object, ISettings p_context = null)
        {
            return SavePublicProperties(p_object, null, p_context).OuterXml;
        }

        /// <summary> Finds or creates a child element named p_elementName in p_xml
        /// (if p_xml is empty, creates it as p_noteXmlTag, otherwise ignores p_noteXmlTag),
        /// and sets the InnerText of that child element to p_text if p_attributeName
        /// is empty; or sets the given attribute to p_text. <para> If p_text is empty,
        /// removes the attribute (or removes the child element if p_attributeName is empty).
        /// If this makes p_xml empty, returns null.
        /// Returns a modified version of p_xml (may be null).
        /// Both p_xml and p_text may be null.</para>
        /// For example, 'ᐸNoteᐳᐸUserNote Text="value of p_text"/ᐳᐸ/Noteᐳ' can be achieved
        /// by p_elementName="UserNote", p_attributeName="Text", p_noteXmlTag="Note" </summary>
        public static string SetTextElementInXml(string p_xml, string p_elementName,
            string p_attributeName, string p_text, string p_noteXmlTag)
        {
            string currentValue = GetTextElementFromXml(p_xml, p_elementName, p_attributeName);
            if (Equals(currentValue, p_text)
                || (String.IsNullOrEmpty(currentValue) && String.IsNullOrEmpty(p_text)))
                return p_xml;

            XmlElement e = String.IsNullOrEmpty(p_xml) ? null : XMLUtils.ParseNode(p_xml);
            e = SetTextElementInXml(e, p_elementName, p_attributeName, p_text, p_noteXmlTag);
            return (e == null) ? null : e.OuterXml;
        }

        public static XmlElement SetTextElementInXml(XmlElement p_xml, string p_elementName,
            string p_attributeName, string p_text, string p_noteXmlTag)
        {
            bool removing = String.IsNullOrEmpty(p_text);
            if (!removing)
                p_xml = p_xml ?? new XmlDocument().CreateElement(p_noteXmlTag);
            else if (p_xml == null)
                return null;
            XmlElement u = GetChildElements(p_xml, p_elementName).FirstOrDefault();
            if (removing)
            {
                if (u != null)
                    p_xml.RemoveChild(u);
                if (!p_xml.HasChildNodes)
                    return null;
            }
            else if (u == null)
            {
                u = p_xml.OwnerDocument.CreateElement(p_elementName);
                p_xml.AppendChild(u);
                if (String.IsNullOrEmpty(p_attributeName))
                    u.InnerText = p_text;
                else
                    u.SetAttribute(p_attributeName, p_text);
            }
            else if (String.IsNullOrEmpty(p_attributeName))
                u.InnerText = p_text;
            else
                u.SetAttribute(p_attributeName, p_text);
            return p_xml;
        }

        /// <summary> Extracts the p_attributeName attribute (or content string
        /// if p_attributeName is empty) of the p_childName child element from p_xml.
        /// </summary>
        public static string GetTextElementFromXml(string p_xml, string p_childName,
            string p_attributeName)
        {
            if (String.IsNullOrEmpty(p_childName))
                throw new ArgumentException();
            if (!String.IsNullOrEmpty(p_xml))
            {
                var stringReader = new System.IO.StringReader(p_xml);
                var xmlReader = new System.Xml.XmlTextReader(stringReader);
                xmlReader.MoveToContent();
                do
                {
                    if (xmlReader.NodeType == System.Xml.XmlNodeType.Element
                        && xmlReader.Name == p_childName)
                    {
                        if (String.IsNullOrEmpty(p_attributeName))
                            return xmlReader.ReadInnerXml();
                        else if (xmlReader.MoveToAttribute(p_attributeName))
                            return xmlReader.Value;
                        else
                            return String.Empty;
                    }
                } while (xmlReader.Read());
            }
			return null;
        }

        public static string GetTextElementFromXml(XmlElement p_xml, string p_childName,
            string p_attributeName)
        {
            if (!String.IsNullOrEmpty(p_childName))
                foreach (XmlElement u in GetChildElements(p_xml, p_childName))
                {
                    if (!String.IsNullOrEmpty(p_attributeName))
                        return u.GetAttribute(p_attributeName);
                    return u.InnerText;
                }
            return null;
        }

		public static Dictionary<string, string> GetTextsOrInnerxmlsFromXml(string p_xml, string p_attributeName)
		{
            if (String.IsNullOrEmpty(p_xml))
                return null;

			var dict = new Dictionary<string, string>();
			var stringReader = new StringReader(p_xml);
			var xmlReader = new XmlTextReader(stringReader);

			xmlReader.MoveToContent();
			do
			{
				if (xmlReader.NodeType == System.Xml.XmlNodeType.Element && xmlReader.Depth == 1)
				{
					string name = xmlReader.Name;
                    string text = null;

					if (!String.IsNullOrEmpty(p_attributeName)
                        && xmlReader.MoveToAttribute(p_attributeName))
						text = xmlReader.Value;
                    else
						text = xmlReader.ReadInnerXml();

					if (!String.IsNullOrEmpty(text))
						dict[name] = text;
				}
			} while (xmlReader.Read());
			return dict;
		}

        /// <summary> Concatenates text from immediate child nodes of type XmlNodeType.Text.
        /// Child nodes of other types are ignored. Returns null if there's no Text child.
        /// p_trim: 0=do not Trim() whitespaces; 1=trim the concatenated result (preserve
        /// intermediate whitespaces); 2=trim items before concatenating </summary>
        public static string GetTextChildrenConcatenated(XmlElement p_element, int p_trim)
        {
            StringBuilder sb = null;
            foreach (XmlNode node in p_element.ChildNodes)
                if (node.NodeType == XmlNodeType.Text)
                {
                    if (sb == null)
                        sb = new StringBuilder();
                    sb.Append(p_trim == 2 ? node.Value.Trim() : node.Value);
                }
            if (sb == null)
                return null;
            string result = sb.ToString();
            return (p_trim == 1) ? result.Trim() : result;
        }

        public static XmlNode AppendChildIfNonNull(this XmlNode p_parent, XmlNode p_child)
        {
            if (p_parent != null && p_child != null)
                return p_parent.AppendChild(p_child);
            return null;
        }


        /// <summary> p_args is passed to XMLUtils.Extend(), see documentation there </summary>
        public static XmlElement ComposeElement(XmlDocument p_doc, object p_elementName,
            params object[] p_args)
        {
            if (p_doc == null)
            {
                XmlElement e = p_args.SelectMany(o => {
                    var e2 = o as XmlElement;
                    if (e2 != null)
                        return Utils.Single(e2);
                    var seq = o as IEnumerable<XmlElement>;
                    if (seq != null)
                        return seq;
                    return Enumerable.Empty<XmlElement>();
                }).FirstOrDefault();
                p_doc = (e == null) ? new XmlDocument() : e.OwnerDocument;
            }
            return Extend(p_doc.CreateElement(Utils.ConvertTo<string>(p_elementName)), p_args);
        }

        public static Func<XmlDocument, XmlElement> ComposeElement(object p_elementName,
            params object[] p_args)
        {
            return (p_doc) => ComposeElement(p_doc, p_elementName.ToString(), p_args);
        }

        /// <summary> Example:
        ///   Extend(existingElement, 
        ///       "attr", DateTime.UtcNow,
        ///       ComposeElement("childName", "attr3", 5),
        ///       new Func≺XmlDocument,XmlElement≻(...),
        ///       seqChildren.Select(item => XmlElement...),
        ///       "childName", XMLUtils.AsPersistable(anXmlPersistable, p_context)
        /// );
        /// </summary>
        public static XmlElement Extend(this XmlElement p_element, params object[] p_args)
        {
            return Extend(p_element, (ISettings)null, p_args);
        }

        public static XmlElement AppendChildElement(this XmlElement p_element,
            string p_elementName, params object[] p_args)
        {
            p_element.AppendChild(p_element.OwnerDocument.CreateElement(p_elementName).Extend(p_args));
            return p_element;
        }

        /// <summary> p_context is used when saving IXmlPersistable values </summary>
        public static XmlElement Extend(this XmlElement p_element, ISettings p_context,
            params object[] p_args)
        {
            if (p_element == null)
                throw new ArgumentNullException();
            IEnumerable<XmlElement> children;
            Func<XmlDocument, XmlElement> childProducer;
            IXmlPersistable xp;
            for (int i = 0; i < p_args.Length; ++i)
            {
                object nameOrChild = p_args[i];
                var child = nameOrChild as XmlElement;
                if (child != null)
                    p_element.AppendChild(child);
                else if (Utils.CanBe(nameOrChild, out childProducer))
                    p_element.AppendChildIfNonNull(childProducer(p_element.OwnerDocument));
                else if (Utils.CanBe(nameOrChild, out children))
                    foreach (XmlElement ch in children)
                        p_element.AppendChildIfNonNull(ch);
                else if (Utils.CanBe(nameOrChild, out xp))
                    SaveXmlPersistable(xp, p_element, p_context);
                else if (nameOrChild == null)
                    continue;
                else if (i + 1 >= p_args.Length)
                    throw new ArgumentException(Utils.FormatInvCult("arg#{0}: missing value", i));
                else if (p_args[++i] != null)
                {
                    string name = Convert.ToString(nameOrChild, Utils.InvCult);
                    if (String.IsNullOrEmpty(name))
                        throw new ArgumentException(Utils.FormatInvCult("arg#{0}: empty name", i - 1));
                    object val = p_args[i];
                    if (null != (xp = val as IXmlPersistable))
                    {   // child
                        p_element.AppendChildIfNonNull(SaveXmlPersistable(xp,
                            p_element.OwnerDocument.CreateElement(name), p_context));
                    }
                    // attribute
                    else if (val is DateTime)
                        p_element.SetAttribute(name, Utils.UtcDateTime2Str((DateTime)val));
                    else if (val is Delegate)
                        p_element.SetAttribute(name, ((Delegate)val).Method.Name);
                    else
                        p_element.SetAttribute(name, Convert.ToString(val, Utils.InvCult));
                }
            }
            return p_element;
        }

        public static XmlElement AddWithDynamicKeyAndComposeXml(this ISettings p_settings,
            object p_object, string p_xmlElementName, string p_xmlAttr, XmlDocument p_xmlDoc)
        {
            if (p_object == null)
                throw new ArgumentNullException();
            XmlElement result = (p_xmlDoc ?? new XmlDocument()).CreateElement(p_xmlElementName);
            result.SetAttribute(p_xmlAttr, p_settings.StoreAtDynamicKey(p_object));
            return result;
        }

        #region IXmlPersistable-related operations
        private const string CLASSNAME_XMLELEMENT = "XmlElement";

        // Common keys for ISettings arguments of IXmlPersistable.Save()/Load() 
        #region XCK I[X]mlPersistable [C]ontext [K]eys

        /// <summary> A DBManager object, or Func&lt;DBManager&gt;
        /// or Func&lt;ISettings, DBManager&gt; </summary>
        public static readonly object XCK_DbManager = new Utils.HashKey("XCK_DbManager");               // much like "new object()" but more debuggable
        public static readonly object XCK_TickerProvider = new Utils.HashKey("XCK_TickerProvider");

        /// <summary> Returns an object that is acceptable for DBManager.FromObject() </summary>
        public static object GetDbManager(ISettings p_context)
        {
            if (p_context == null)
                return null;
            object o = p_context[XCK_DbManager];
            var fctx = o as Func<ISettings, DBManager>;
            return (fctx != null) ? fctx(p_context) : o;
        }

        public static ITickerProvider GetTickerProvider(ISettings p_context)
        {
            return (p_context == null) ? null : p_context[XCK_TickerProvider] as ITickerProvider;
        }

        /// <summary> Returns a new I[X]mlPersistable [C]ontext initialized with the arguments.
        /// The following arguments are accepted:<para>
        /// - DBManager: all forms supported by DBManager.FromObject() are accepted</para><para>
        /// - IContext: DBManager and TickerProvider are extracted</para>
        /// - ITickerProvider </summary>
        public static ISettings MakeXC(params object[] p_args)
        {
            if (p_args == null || p_args.Length == 0)
                return null;
            var result = new DefaultSettings();
            foreach (object arg in p_args)
            {
                ITickerProvider tp = null;
                object dbManager = null;
                var ctx = arg as IContext;
                if (ctx != null)
                {
                    dbManager = ctx.DBManager;
                    tp = ctx.TickerProvider;
                }
                else if ((tp = arg as ITickerProvider) == null)
                    dbManager = DBManager.FromObject(arg, p_throwOnNull: false);
                if (dbManager != null)
                    result[XCK_DbManager] = dbManager;
                if (tp != null)
                    result[XCK_TickerProvider] = tp;
            }
            return result;
        }
        #endregion

        public class PublicFieldsPropertiesXmlPersistable : IXmlPersistable
        {
            /// <summary> Properties having no setter or no public getter are not saved </summary>
            public virtual XmlElement Save(XmlElement p_element, ISettings p_context)
            {
                return XMLUtils.SavePublicProperties(this, p_element, p_context);
            }
            public virtual void Load(XmlElement p_element, ISettings p_context)
            {
                XMLUtils.ParsePublicProperties(this, p_element, p_context);
            }
            public override string ToString()
            {
                return Save(null, null).OuterXml;
            }
        }

        public static XmlElement Save(this IXmlPersistable p_object, XmlDocument p_doc, ISettings p_context = null)
        {
            return p_object.Save(p_doc.CreateElement(ComposeTagNameFromType(p_object.GetType())), p_context);
        }
        public static XmlElement SaveXmlPersistable(this IXmlPersistable p_object, 
            XmlElement p_node, ISettings p_context)
        {
            if (p_object == null)
                return null;
            p_node.SetAttribute("class", p_object.GetType().ToString());    // important to do it before p_object.Save(). That may want to modify "class"
            return p_object.Save(p_node, p_context);
        }

        public static void SaveAnyType(object p_obj, string p_filePath,
            ISettings p_saveLoadContext = null)
        {
            XmlElement root = new XmlDocument().CreateElement(ComposeTagNameFromType(p_obj.GetType()));
            if (p_obj is IXmlPersistable || p_obj is XmlElement || p_obj is IConvertible)
                root = SaveAnyType(p_obj, root.OwnerDocument, () => root, p_saveLoadContext);
            else
                SaveEnumerable((System.Collections.IEnumerable)p_obj, root, p_saveLoadContext, null);
            SaveNode(root, p_filePath);
        }

        // Supported types:
        // IConvertible: create XmlElement, stringge konvertaljuk InnerText-be.
        //   (nincs "class" attributum)
        // XmlElement: clone (import into p_doc) (a neve nem szamit).
        //   Ha van benne "class" attributum, akkor vki trukkozik (megengedjuk,
        //   akkor fog mukodni visszatoltesnel ha az erteke egy IXmlPersistable
        //   implementacio). Ha nincs benne "class" attributum, akkor teszunk
        //   bele egyet, spec.ertekkel, h betoltesnel meg tudjuk kulonboztetni
        //   az IConvertible esettol:
        //        class = "XmlElement"   // ld. CLASSNAME_XMLELEMENT
        // IXmlPersistable: create XmlElement, SaveAnyType()
        // Array/ICollection<T>/stb.: azokat a tipusokat fogadjuk el, amire
        //   van wrapper (IXmlWrapper). Igy betolteskor a wrapper osztaly 
        //   fog peldanyositodni, es az o betoltoje fut le. Ez egyszerusiti
        //   a betolto rutint (LoadEnumerable() -> LoadAnyType()) mert csak 
        //   IXmlWrapper-re kell ravizsgalni es ha az, akkor a wrapper objektum
        //   helyett a WrappedObject-t visszaadni.
        public static XmlElement SaveAnyType(object p_obj, XmlDocument p_doc,
            Func<XmlElement> p_element, ISettings p_context)
        {
            if (p_obj == null)
                return null;
            object o = p_obj;
            XmlElement e = null;
            if (o is IConvertible)                  // Save as string
            {
                string s;
                if (o is DateTime)
                {
                    DateTime d = (DateTime)o;
                    if (d.TimeOfDay.Ticks < TimeSpan.TicksPerSecond)
                        s = d.ToString("yyyy'-'MM'-'dd", Utils.InvCult);
                    else
                        s = d.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss", Utils.InvCult);
                }
                else
                    s = Convert.ToString(o, Utils.InvCult);
                e = p_element();
                e.InnerText = s;
            }
            else if (o is XmlElement)               // Save as XmlElement
            {
                e = (XmlElement)o;
                if (e.OwnerDocument != p_doc)
                    e = (XmlElement)p_doc.ImportNode(e, true);
                if (String.IsNullOrEmpty(e.GetAttribute("class")))
                    e.SetAttribute("class", CLASSNAME_XMLELEMENT);
            }
            else                                    // Save as IXmlPersistable
            {
                var p = o as IXmlPersistable;
                if (p == null)                      // Try to wrap
                    foreach (Func<object, IXmlWrapper> wrapIfSupported in g_xmlWrappers)
                        if (null != (p = wrapIfSupported(o)))
                            break;
                if (p != null)
                    e = SaveXmlPersistable(p, p_element(), p_context);
                //else
                //    Trace.WriteLine("*** warning in XMLUtils.SaveEnumerable():"
                //        + " skipping object of unsupported type " + o.GetType().ToString());
            }
            return e;
        }

        public static T CreateAndLoad<T>(XmlElement p_element, ISettings p_context)
            where T : IXmlPersistable, new()
        {
            T result = new T();
            result.Load(p_element, p_context);
            return result;
        }

        public static TExpected LoadAnyType<TExpected>(XmlElement p_element, ISettings p_context)
        {
            return (TExpected)LoadAnyType(typeof(TExpected), p_element, p_context);
        }

        /// <summary> The returned object is guaranteed to be of type p_expected, or null. 
        /// Uses Utils.ChangeType() for the conversion, with invariant culture.
        /// </summary>
        public static object LoadAnyType(Type p_expected, XmlElement p_element, ISettings p_context)
        {
            if (p_element == null)
                return null;
            if (p_expected == null)
                p_expected = typeof(object);

            object result = null;
            string clsName = p_element.GetAttribute("class");
            if (String.IsNullOrEmpty(clsName))  // no "class" attribute: use InnerText as plain string
            {
            // IConvertible
                if (!typeof(IConvertible).IsAssignableFrom(p_expected)
                    && !typeof(object).Equals(p_expected))
                    throw new XmlException("missing 'class' attribute at " 
                        + p_element.GetDebugPath());
                result = p_element.InnerText;
            }
            else if (clsName == CLASSNAME_XMLELEMENT)
            // XmlElement
                result = p_element;
            else
            {
            // something with 'class' attribute
                Type type = Utils.FindTypeInAllAssemblies(clsName, null);
                if (type == null)
                    throw new XmlException(String.Format("{0}: unknown type: {1}",
                        p_element.GetDebugPath(), clsName));
                if (typeof(IXmlWrapper).IsAssignableFrom(type))
                {
                    IXmlWrapper w = Utils.CreateObject<IXmlWrapper>(type);
                    w.Load(p_element, p_context);           // may throw Exception (any)
                    result = w.WrappedObject;
                }
                else if (!p_expected.IsAssignableFrom(type))
                    throw new XmlException(String.Format("{0}: type {1} is not a {2}",
                        p_element.GetDebugPath(), clsName, p_expected.FullName));
                else
                {
                    // Call the default ctor of 'type'
                    result = Activator.CreateInstance(type);
                    var persistable = result as IXmlPersistable;
                    if (persistable == null)
                        throw new XmlException(String.Format("{0}: unsupported type: {1}",
                            p_element.GetDebugPath(), (result == null ? type : result.GetType()).FullName));
                    persistable.Load(p_element, p_context);
                }
            }
            Utils.ChangeType(ref result, p_expected, null);  // may throw InvalidCastException
            return result;
        }

        public static IEnumerable<T> LoadEnumerable<T>(XmlElement p_node,
            ISettings p_context, bool p_returnNulls, Func<XmlElement, ISettings, T> p_customLoader)
        {
            XmlElement element;
            foreach (object o in p_node.ChildNodes)
                if (null != (element = (o as XmlElement)))
                {
                    T result = p_customLoader(element, p_context);
                    if (p_returnNulls || result != null)
                        yield return result;
                }
        }

        public static System.Collections.IEnumerable LoadEnumerable(XmlElement p_node,
            ISettings p_context)
        {
            XmlElement element;
            foreach (object o in p_node.ChildNodes)
                if (null != (element = (o as XmlElement)))
                        yield return LoadAnyType(null, element, p_context);
        }

        /// <summary> Returns the number of elements saved. </summary>
        /// <remarks> Processes p_seq in blocks: p_elementNames.Length consecutive
        /// items at once. If any item of the block is not supported by SaveAnyType()
        /// (e.g. null) then skips the whole block. Otherwise creates new XmlElement
        /// for every item of the block (using names from p_elementNames[]) and saves
        /// these to p_node using p_node.AppendChild().
        /// When the name in p_elementNames[] is empty, uses ComposeTagNameFromType()
        /// to generate name for the item. (Often the names are irrelevant for the
        /// program, mostly used to facilitate human readability / debugging.)
        /// For example, p_elementNames=={"Key", "Value"} produces children like this:
        ///     ᐸKeyᐳ...ᐸ/KeyᐳᐸValueᐳ...ᐸ/Valueᐳ
        /// </remarks>
        public static int SaveEnumerable(System.Collections.IEnumerable p_seq,
            XmlNode p_node, ISettings p_saveLoadContext, string[] p_elementNames)
        {
            if (p_elementNames == null || p_elementNames.Length == 0)
                p_elementNames = new string[1];
            XmlElement[] fields = new XmlElement[p_elementNames.Length];
            XmlDocument doc = p_node.OwnerDocument ?? (XmlDocument)p_node;
            string name = null;
            Func<XmlElement> elementFactory = () => doc.CreateElement(name);
            bool ok = true;
            int i = 0, nSaved = 0;
            foreach (object o in p_seq)
            {
                XmlElement e = null;
                if (ok && o != null)
                {
                    name = p_elementNames[i];
                    if (String.IsNullOrEmpty(name))
                        name = ComposeTagNameFromType(o.GetType());
                    e = SaveAnyType(o, doc, elementFactory, p_saveLoadContext);
                }
                fields[i++] = e;
                ok &= (e != null);
                if (i == fields.Length)
                {
                    for (i = 0; ok && i < fields.Length; ++i, ++nSaved)
                        p_node.AppendChild(fields[i]);
                    i = 0;
                    ok = true;
                }
            }
            return nSaved;
        }

        public static IXmlPersistable AsPersistable(object p_obj)
        {
            return new PersistableHelper { m_obj = p_obj };
        }
        public static IXmlPersistable AsPersistable(object p_obj, ISettings p_context)
        {
            return new PersistableHelper { m_obj = p_obj, m_customCtx = p_context };
        }
        public static IXmlPersistable SuppressClassAttr(object p_obj)
        {
            return new PersistableHelper { m_obj = p_obj, m_suppressClassAttribute = true };
        }
        private class PersistableHelper : IXmlPersistable
        {
            internal object m_obj;
            internal ISettings m_customCtx;
            internal bool m_suppressClassAttribute;
            public XmlElement Save(XmlElement p_element, ISettings p_context)
            {
                p_element.RemoveAttribute("class");
                XmlElement result = SaveAnyType(m_obj, p_element.OwnerDocument, () => p_element, 
                    m_customCtx ?? p_context);
                if (m_suppressClassAttribute)
                    result.RemoveAttribute("class");
                return result;
            }
            public void Load(XmlElement p_element, ISettings p_context)
            {
                throw new NotSupportedException();
            }
        }


        #region IXmlWrapper and implementations
        public interface IXmlWrapper : IXmlPersistable
        {
            object WrappedObject { get; }
        }

        /// <summary> List of delegates that wrap the input object into 
        /// a newly created IXmlWrapper when the type of the input object
        /// is supported by that wrapper implementation.
        /// Delegates are visited in order, to allow wrappers of more 
        /// specific types precede the wrappers of general types. 
        /// For example, the wrapper of Arrays must precede the wrapper
        /// of generic ICollections because the latter utilizes the 
        /// ICollection.Add() method which throws exception in Arrays.
        /// </summary>
        public static SynchronizedCollection<Func<object, IXmlWrapper>> g_xmlWrappers
            = new SynchronizedCollection<Func<object, IXmlWrapper>>(new object(),
                new Func<object, IXmlWrapper>[] {
                    TypeXmlWrapper.CreateIfType,
                    ArrayXmlWrapper.CreateIfArray,
                    CollectionXmlWrapper.CreateIfCollection,
                    KeyValueXmlWrapper.CreateIfKeyValuePair,
                    DelegateXmlWrapper.CreateIfStaticDelegate
                });

        private static Type FindWrappedType(XmlElement p_element, string p_attribute,
            out string p_clsName)
        {
            p_clsName = GetRequiredAttribute(p_element, p_attribute);
            Type type = Utils.FindTypeInAllAssemblies(p_clsName, null);
            if (type == null)
                ThrowInvalidType(p_element, p_clsName);
            return type;
        }
        private static Type FindWrappedType(XmlElement p_element, out string p_clsName)
        {
            return FindWrappedType(p_element, "wrappedClass", out p_clsName);
        }
        private static void ThrowInvalidType(XmlNode p_node, object p_type)
        {
            throw new XmlException(String.Format("{0}: unknown type: \"{1}\"",
                p_node.GetDebugPath(), p_type));
        }

        private class TypeXmlWrapper : IXmlWrapper
        {
            public object WrappedObject { get; private set; }
            public static TypeXmlWrapper CreateIfType(object p_type)
            {
                return (p_type is Type) ? new TypeXmlWrapper { WrappedObject = p_type } : null;
            }
            public XmlElement Save(XmlElement p_element, ISettings p_context)
            {
                p_element.SetAttribute("wrappedClass", WrappedObject.ToString());
                return p_element;
            }
            public void Load(XmlElement p_element, ISettings p_context)
            {
                string clsName;
                WrappedObject = FindWrappedType(p_element, out clsName);
            }
        }

        private class DelegateXmlWrapper : IXmlWrapper
        {
            public object WrappedObject { get; private set; }
            public static DelegateXmlWrapper CreateIfStaticDelegate(object p_obj)
            {
                var d = p_obj as Delegate;
                if (d != null && d.Target == null)
                    return new DelegateXmlWrapper { WrappedObject = p_obj };
                return null;
            }
            public XmlElement Save(XmlElement p_element, ISettings p_context)
            {
                var d = WrappedObject as Delegate;
                p_element.SetAttribute("delegateType", d.GetType().ToString());
                p_element.SetAttribute("targetClass", d.Method.DeclaringType.ToString());
                p_element.SetAttribute("methodName", d.Method.Name);
                return p_element;
            }
            public void Load(XmlElement p_element, ISettings p_context)
            {
                string s;
                Type dt = FindWrappedType(p_element, "delegateType", out s);
                if (!typeof(Delegate).IsAssignableFrom(dt))
                    ThrowInvalidType(p_element, s);
                Type t = FindWrappedType(p_element, "targetClass", out s);
                s = GetRequiredAttribute(p_element, "methodName");
                WrappedObject = Delegate.CreateDelegate(dt, t, s);
            }
        }

        private class ArrayXmlWrapper : IXmlWrapper
        {
            public object WrappedObject { get; private set; }

            public static ArrayXmlWrapper CreateIfArray(object p_array)
            {
                if (p_array is Array)
                    return new ArrayXmlWrapper { WrappedObject = p_array };
                return null;
            }
            public XmlElement Save(XmlElement p_element, ISettings p_context)
            {
                p_element.SetAttribute("wrappedClass", WrappedObject.GetType().ToString());
                int nSaved = SaveEnumerable((System.Collections.IEnumerable)WrappedObject, 
                    p_element, p_context, null);
                // nSaved == 0 when the element type is not supported (cannot be saved)
                // or the array is full of nulls (e.g. Runner[]) or empty
                return nSaved > 0 ? p_element : null;
            }
            public void Load(XmlElement p_element, ISettings p_context)
            {
                string clsName;
                Type type = FindWrappedType(p_element, out clsName);
                if (!typeof(Array).IsAssignableFrom(type))
                    ThrowInvalidType(p_element, clsName);
                var ctor = type.GetConstructor(new Type[] { typeof(int) });
                type = type.GetElementType();

                var buff = new System.Collections.ArrayList();
                foreach (object o in LoadEnumerable(p_element, p_context))
                {
                    object tmp = o;
                    Utils.ChangeType(ref tmp, type, null);
                    buff.Add(tmp);
                }
                Array a = (Array)ctor.Invoke(new object[] { buff.Count });
                for (int i = buff.Count - 1; i >= 0; --i)
                    a.SetValue(buff[i], i);
                WrappedObject = a;
            }
        }

        private class CollectionXmlWrapper : IXmlWrapper
        {
            public object WrappedObject { get; private set; }

            public static CollectionXmlWrapper CreateIfCollection(object p_coll)
            {
                Type[] t = (p_coll == null) ? Type.EmptyTypes
                    : Utils.GetGenericTypeArgs(p_coll.GetType(), typeof(ICollection<>));
                if (0 < t.Length)
                    return new CollectionXmlWrapper { WrappedObject = p_coll };
                return null;
            }
            public XmlElement Save(XmlElement p_element, ISettings p_context)
            {
                p_element.SetAttribute("wrappedClass", WrappedObject.GetType().ToString());
                int nSaved = SaveEnumerable((System.Collections.IEnumerable)WrappedObject,
                    p_element, p_context, null);
                // nSaved == 0 when the element type is not supported (cannot be saved)
                // or the collection is empty or contains nulls only
                return nSaved > 0 ? p_element : null;
            }
            public void Load(XmlElement p_element, ISettings p_context)
            {
                string clsName;
                Type type = FindWrappedType(p_element, out clsName);

                // Instantiate the object by the default ctor,
                // then load the items and add them one by one
                var coll = Utils.CreateObject<object>(type);
                Type itemType = null;
                MethodInfo addMethod = null;
                object[] tmp = { null };
                foreach (object o in LoadEnumerable(p_element, p_context))
                {
                    if (addMethod == null)
                    {
                        // We are about to call ICollection<T>.Add(T)
                        // For this, first we need to detect T (-> itemType)
                        itemType = o.GetType();
                        Type[] actual = Utils.GetGenericTypeArgs(type, typeof(ICollection<>));
                        int i = actual.Length - 1;
                        for (int j = 1; j != 0 && i >= 0 && !itemType.IsAssignableFrom(actual[i]); i += j)
                            j = -i >> 31;   // (i > 0) ? -1 : 0;
                        // Use actual[0] if itemType.IsAssignableFrom()==false for all
                        if (0 <= i)
                            addMethod = type.GetMethod("Add", new Type[] { itemType = actual[i] });
                        if (addMethod == null)
                            ThrowInvalidType(p_element, clsName);
                    }
                    // Potential conversion from string. 
                    // Note: Utils.ChangeType() can convert to bool and Enum, too.
                    tmp[0] = o;
                    Utils.ChangeType(ref tmp[0], itemType, null);
                    addMethod.Invoke(coll, tmp);
                }
                WrappedObject = coll;
            }
        }

        private class KeyValueXmlWrapper : IXmlWrapper
        {
            public object WrappedObject { get; private set; }

            public static KeyValueXmlWrapper CreateIfKeyValuePair(object p_kv)
            {
                if (p_kv != null)
                {
                    Type t = p_kv.GetType();
                    if (t.IsGenericType && t.GetGenericTypeDefinition().Equals(
                            typeof(KeyValuePair<int, int>).GetGenericTypeDefinition()))
                        return new KeyValueXmlWrapper { WrappedObject = p_kv };
                }
                return null;
            }
            public XmlElement Save(XmlElement p_element, ISettings p_context)
            {
                Type t = WrappedObject.GetType();
                p_element.SetAttribute("wrappedClass", t.ToString());
                return SaveEnumerable(new object[] {
                        Utils.GetValueOfMember<object>("Key", WrappedObject),
                        Utils.GetValueOfMember<object>("Value", WrappedObject)
                    },
                    p_element, p_context, new string[] { "Key", "Value" }) 
                    == 0 ? null : p_element;  // skip if empty because Load would fail on it
            }
            public void Load(XmlElement p_element, ISettings p_context)
            {
                string clsName;
                Type type = FindWrappedType(p_element, out clsName);
                Type[] typeArgs = null;
                if (type.IsGenericType)
                    typeArgs = type.GetGenericArguments();
                ConstructorInfo ctor = null;
                if (typeArgs != null && typeArgs.Length == 2)
                    ctor = type.GetConstructor(typeArgs);
                if (ctor == null)
                    ThrowInvalidType(p_element, clsName);
                object[] args = { null, null };
                int i = 0;
                foreach (object o in LoadEnumerable(p_element, p_context))
                {
                    args[i] = o;
                    Utils.ChangeType(ref args[i], typeArgs[i], null);
                    if (++i == args.Length)
                        break;
                }
                WrappedObject = ctor.Invoke(args);
            }
        }
        #endregion
        #endregion
    }

    public class XmlBuilder
    {
        XmlElement m_element;
        public XmlBuilder(XmlDocument p_doc, object p_elementName)
        {
            m_element = p_elementName as XmlElement
                ?? (p_doc ?? new XmlDocument()).CreateElement(Utils.ConvertTo<string>(p_elementName));
        }
        /// <summary> Adds a child element </summary>
        public XmlBuilder this[object p_elementName]
        {
            get
            {
                XmlElement e = p_elementName as XmlElement;
                if (e == null)
                    e = m_element.OwnerDocument.CreateElement(
                            Utils.ConvertTo<string>(p_elementName));
                m_element.AppendChild(e);
                m_element = e;
                return this;
            }
        }
        public XmlBuilder Attribute(object p_name, object p_value)
        {
            string name  = Utils.ConvertTo<string>(p_name);
            string value = Utils.ConvertTo<string>(p_value);
            if (value != null)
            {
                if (String.IsNullOrEmpty(name))
                    throw new XmlException(m_element.GetDebugPath() + ": attribute name is empty");
                m_element.SetAttribute(name, value);
            }
            return this;
        }
        /// <summary> Marks the end of the current XmlElement: moves to the parent element </summary>
        public XmlBuilder End() { return End(null); }
        public XmlBuilder End(object p_name)
        {
            if (p_name != null)
            {
                string name = Utils.ConvertTo<string>(p_name);
                if (name != m_element.Name)
                    throw new XmlException(String.Format("{0}: </{2}> instead of </{1}>",
                        m_element.GetDebugPath(), m_element.Name, name));
            }
            m_element = (XmlElement)m_element.ParentNode;
            return this;
        }
        public XmlElement Current { get { return m_element; } }
        public XmlElement Root
        {
            get
            {
                XmlElement e = m_element, parent;
                while (null != (parent = e.ParentNode as XmlElement))
                    e = parent;
                return e;
            }
        }
        public static implicit operator XmlElement(XmlBuilder p_this) { return p_this.Root; }
    }


    /// <summary> Specifies a value that is saved instead of null,
    /// and parsed to null when loading. The value should be convertible
    /// to string. </summary>
    public sealed class XmlNullReplacementAttribute : Attribute
    {
        public object Null { get; set; }
        public XmlNullReplacementAttribute(object p_nullValue) { Null = p_nullValue; }
    }
}
