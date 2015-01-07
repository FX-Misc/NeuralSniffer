using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;

namespace HQCommon
{
    public static partial class Utils
    {
		public static void LaunchBrowser(string p_url)
		{
			try
			{
				ProcessStartInfo psi = new ProcessStartInfo(p_url);
				psi.RedirectStandardOutput = false;
				psi.UseShellExecute = true;
				psi.WindowStyle = ProcessWindowStyle.Normal;
				Process.Start(psi);
			}
			catch (Exception e)
			{
                Trace.WriteLine(Logger.FormatExceptionMessage(e, true, "in {0}(\"{1}\")",
                    GetQualifiedMethodName(System.Reflection.MethodBase.GetCurrentMethod()),
                    p_url));
			}
		}

        /// <summary> Returns true if the command was launched successfully
        /// (even if stopped later with error) </summary>
        public static bool RunCmdLineTool(string p_program, string p_args, string p_startDir)
        {
            return RunCmdLineTool(p_program, p_args, p_startDir, null).HasValue;
        }

        /// <summary> Returns the exit code of the command if it was launched
        /// successfully, otherwise returns null. </summary>
        public static int? RunCmdLineTool(string p_program, string p_args, string p_startDir,
            ProcessPriorityClass? p_priority, DataReceivedEventHandler p_stdOut = null, 
            DataReceivedEventHandler p_stdErr = null)
        {
            if (String.IsNullOrEmpty(p_program) || !File.Exists(p_program))
            {
                string tmp = FindInPATH(p_program);
                if (String.IsNullOrEmpty(tmp))
                {
                    Utils.Logger.Error("*** Error: executable not found: \"{0}\"", p_program);
                    return null;
                }
                p_program = tmp;
            }
            ProcessStartInfo args = new ProcessStartInfo(p_program, p_args);
            args.CreateNoWindow = true;
            args.UseShellExecute = false;
            args.WindowStyle = ProcessWindowStyle.Hidden;
            if (!String.IsNullOrEmpty(p_startDir))
            {
                args.WorkingDirectory = p_startDir;
                Utils.Logger.Verbose("Running: {0} {1}\n\tin directory: {2}",
                                    p_program, p_args, p_startDir);
            }
            else
            {
                Utils.Logger.Verbose("Running: {0} {1}\n\tin directory: {2}",
                                    p_program, p_args, Environment.CurrentDirectory);
            }
            try
            {
                var p = new Process();
                if (p_stdOut != null)
                {
                    args.RedirectStandardOutput = true;
                    p.OutputDataReceived += p_stdOut;
                }
                if (p_stdErr != null)
                {
                    args.RedirectStandardError = true;
                    p.ErrorDataReceived += p_stdErr;
                }
                p.StartInfo = args;
                p.Start();
                if (args.RedirectStandardOutput)
                    p.BeginOutputReadLine();
                if (args.RedirectStandardError)
                    p.BeginErrorReadLine();
                if (p_priority.HasValue && p_priority != ProcessPriorityClass.Normal)
                    try { p.PriorityClass = p_priority.Value; } catch { }   // the process may have already exited (errors/email#54d06c08)
                if (ApplicationState.IsExiting && ApplicationState.IsUrgentExit && ApplicationState.IsOtherThreadExiting)
                {
                    Utils.Logger.Verbose("{0}(): Abandoning the above process because of IsUrgentExit", System.Reflection.MethodInfo.GetCurrentMethod().Name);
                    return null;
                }
                if (ApplicationState.IsExiting)
                    p.WaitForExit();
                else using (ThreadManager.Singleton.RetardApplicationExit(String.Format("{0}(\"{1}\",\"{2}\",...)", System.Reflection.MethodInfo.GetCurrentMethod().Name, p_program, p_args)))
                {
                    var m = new System.Threading.ManualResetEvent(true) { SafeWaitHandle = new Microsoft.Win32.SafeHandles.SafeWaitHandle(p.Handle, false) };
                    if (0 != System.Threading.WaitHandle.WaitAny(new System.Threading.WaitHandle[] { m, ApplicationState.Token.WaitHandle }))
                        ApplicationState.AssertOtherThreadNotExiting();
                }
                return p.HasExited ? p.ExitCode : (int?)null;
            }
            catch (Exception e)
            {
                Utils.Logger.PrintException(e, true, "while executing {0}\n\t({1} {2})",
                    Path.GetFileName(p_program), p_program, p_args);
                return null;
            }
        }

        public static string JoinCmdLineArgs(params string[] p_args)
        {
            var result = new StringBuilder();
            if (p_args != null)
            {
                foreach (string w in p_args)
                {
                    string arg = w;
                    if (System.Text.RegularExpressions.Regex.IsMatch(arg, @"[\s^&|()*?""]"))
                        arg = '"' + arg.Replace("\"", "\"\"\"") + '"';
                    if (result.Length > 0)
                        result.Append(' ');
                    result.Append(arg);
                }
            }
            return result.ToString();
        }

        /// <summary> Looks for p_program in the PATH (environment variable).
        /// If p_program does not have an extension, tries PATHEXT extensions. </summary>
        public static string FindInPATH(string p_program)
        {
            if (String.IsNullOrEmpty(p_program))
                return null;
            string[] extensions = { Path.GetExtension(p_program) };
            if (String.IsNullOrEmpty(extensions[0]))
            {
                extensions = Utils.Split(Environment.GetEnvironmentVariable("pathext"), Path.PathSeparator.ToString());
                for (int i = extensions.Length - 1; i >= 0; --i)
                    extensions[i] = p_program + extensions[i];
            }
            else
            {
                extensions[0] = p_program;
            }
            foreach (string dir in GetPATHfolders())
                foreach (string extfn in extensions)
                {
                    string fn = Path.Combine(dir, extfn);
                    if (File.Exists(fn))
                        return fn;
                }
            return null;
        }

        public static string[] GetPATHfolders()
        {
            return Utils.Split(Environment.GetEnvironmentVariable("PATH").Replace("\"", String.Empty), Path.PathSeparator.ToString());
        }

        /// <summary> Fixes that Path.Combine(@"G:\a\b\c", @"\ab.txt") would be @"\ab.txt" instead of @"G:\ab.txt" </summary>
        public static string PathJoin(params string[] p_paths)
        {
            if (p_paths == null || p_paths.Length < 1)
                return String.Empty;
            string result = p_paths[0];
            for (int i = 0, n = p_paths.Length; ++i < n; )
            {
                string a = p_paths[i];
                if (String.IsNullOrEmpty(a))
                    continue;
                if (!Path.IsPathRooted(a))  // note: IsPathRooted('\ab.txt') == true
                    result = Path.Combine(result, a);
                else if (1 < a.Length && ((a[1] == ':' && Char.IsLetter(a[0])) || a.StartsWith(@"\\")))
                    result = a;
                else        // because Path.Combine(@"G:\a\b\c", @"\ab.txt") would be @"\ab.txt" instead of @"G:\ab.txt"
                    result = Path.GetPathRoot(result).TrimEnd('\\') + a;
            }
            return result;
        }

        /// <summary> Returns null if there's no parent (or p_pathOrFilename is null) </summary>
        public static string GetParent(string p_pathOrFilename)
        {
            // Path.GetDirectoryName() would throw an exception without this test
            return String.IsNullOrEmpty(p_pathOrFilename) ? null : Path.GetDirectoryName(p_pathOrFilename);
        }

        public static string GetExeDir()
        {
            string s = GetExePath();
            return String.IsNullOrEmpty(s) ? null : Path.GetDirectoryName(s);
        }

        /// <summary> Returns "xy.exe" </summary>
        public static string GetExeName()
        {
            string s = GetExePath();
            return String.IsNullOrEmpty(s) ? String.Empty : Path.GetFileName(s);
        }

        public static string GetExePath()
        {
            // TODO: consider using AppDomain.CurrentDomain.BaseDirectory;

            // When running in ASP.NET, instead of returning the path of the webserver executable,
            // return full path to ~/bin/SiteName (where SiteName need not exist in filesystem).
            if (IsWebApp())
                return new Func<string>(delegate {  // delegate is used to avoid loading System.Web.dll when JITing this method
                    string s = System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath.Replace('/',' ');
                    string p = System.Web.Hosting.HostingEnvironment.ApplicationPhysicalPath;
                    string bin = Path.Combine(p, "bin");
                    return Path.Combine((Directory.Exists(bin) ? bin : p), s);
                })();
            System.Reflection.Assembly a = System.Reflection.Assembly.GetEntryAssembly();
            // TODO: if null, you could try sth like System.Diagnostics.TraceInternal.AppName (see its code in Reflector)
            if (a == null)
                a = System.Reflection.Assembly.GetExecutingAssembly();
            return a.Location;
        }

        /// <summary> Returns System.Web.Hosting.HostingEnvironment.IsHosted, or false if System.Web.dll is not loaded.
        /// Side effect: initializes Utils.g_AspNetHostingEnv </summary>
        public static bool IsWebApp()
        {
            do
            {
                Type t = g_AspNetHostingEnv as Type;
                if (t != null)
                    return (t != typeof(Utils));
                object sync = new object(), old;
                lock ((old = System.Threading.Interlocked.CompareExchange(ref g_AspNetHostingEnv, sync, null)) ?? sync)
                    if (old == null)
                    {
                        System.Reflection.Assembly a = AppDomain.CurrentDomain.GetAssemblies()
                            .Where(asm => asm.GetName().Name == "System.Web").FirstOrDefault();
                        if (a != null)
                            t = a.GetType("System.Web.Hosting.HostingEnvironment");
                        bool isHosted;
                        g_AspNetHostingEnv = (t != null && Utils.GetValueOfMember("IsHosted", t, out isHosted) != null && isHosted) ? t
                            : typeof(Utils);
                    }
            } while (true);
        }
        static object g_AspNetHostingEnv;   // typeof(Utils) or typeof(System.Web.Hosting.HostingEnvironment) (or null or temporary object)

        /// <summary> Case-insensitive ordinal comparison, works for nulls </summary>
        public static bool PathEquals(string p_fn1, string p_fn2)
        {
            return (p_fn1 == null) ? p_fn2 == null : p_fn1.Equals(p_fn2, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary> If the file p_fn exists, this method inserts a '.' followed by a number to the
        /// filename -- or increments the number <it>if it is already there</it> --, until a non-existing 
        /// filename is produced. By default the file extension is not changed, numbering is 
        /// performed just before the extension. Example: trace.log -> trace.01.log -> trace.02.log<para>
        /// p_flags is a combination of the following values:</para><para>
        /// 1 (bit0): create the file (zero length). Default: don't create.</para><para>
        /// 2 (bit1): put the number to the end of p_fn (after the extension)</para>
        /// </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static string IncrementFileName(string p_fn, int p_flags)
        {
            bool isEnd = ((p_flags & 2) == 2);
            string ext = isEnd ? String.Empty : Path.GetExtension(p_fn);
            string result = new AutoIncrementedName(fn => File.Exists(fn + ext)) {
                FirstNumber = 1,
                NumberFormat = ".{0:d2}",
                DetectNumbersAtEndMaxWidth = 2
            }.Generate(isEnd ? p_fn : Path.Combine(Path.GetDirectoryName(p_fn),
                Path.GetFileNameWithoutExtension(p_fn))) + ext;
            if ((p_flags & 1) == 1)
                File.WriteAllBytes(result, new byte[0]);
            return result;
        }

        /// <summary> If the file p_fn exists, this method adds a '.' followed by a number
        /// to the filename, and increments it until a non-existing filename is produced.
        /// If there's already a number in p_fn, this method adds another.
        /// Example: trace.log -> trace.01.log -> trace.01.01.log <para>
        /// p_flags is a combination of the following values: </para><para>
        /// 1 (bit0): create an empty file with the returned name (default: don't) </para><para>
        /// 2 (bit1): number p_fn at the end (default: insert the number before the extension)
        /// </para></summary>
        public static string AddNumberToFileName(string p_fn, int p_flags)
        {
            string ext = ((p_flags & 2) == 0) ? Path.GetExtension(p_fn) : String.Empty;
            return AddNumberToFileName(p_fn, (p_flags & 1) == 1, (fn, number) =>
            {
                string nr = String.Format(".{0:d2}{1}", number, ext);
                return String.IsNullOrEmpty(ext) ? fn + nr : Path.ChangeExtension(fn, nr);
            });
        }

        /// <summary> This overload allows you full control over the formatting of the filename. </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static string AddNumberToFileName(string p_fn, bool p_create, 
            Func<string, int, string> p_composeNewFilename)
        {
            string result = p_fn;
            for (int number = 1; true; ++number)
            {
                if (p_create)
                    try 
                    {
                        using (File.Open(result, FileMode.CreateNew))
                            break;
                    }
                    catch (IOException) { }
                else if (!File.Exists(result))
                    break;
                result = p_composeNewFilename(p_fn, number);
            }
            return result;
        }

        public static Exception EnableNtfsCompression(string p_filename)
        {
            try { using (FileStream f = File.Open(p_filename, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) {
                const int FSCTL_SET_COMPRESSION = 0x9C040;
                short COMPRESSION_FORMAT_DEFAULT = 1;
                int lpBytesReturned = 0;
                int result = Win32.DeviceIoControl(f.SafeFileHandle, FSCTL_SET_COMPRESSION,
                    ref COMPRESSION_FORMAT_DEFAULT, 2 /*sizeof(short)*/, IntPtr.Zero, 0,
                    ref lpBytesReturned, IntPtr.Zero);
                return (result == 0) ? null : new System.ComponentModel.Win32Exception(
                    System.Runtime.InteropServices.Marshal.GetLastWin32Error());
            } } catch (Exception e) { return e; }
        }

        /// <summary> p_dest may be either a file name or a folder.
        /// If it is a folder, file name of p_src is appended automatically.<para>
        /// If the destination file exists, p_whatIfDstExists specifies what to do:
        /// {null or false}=delete the destination;
        /// true=increment dst name and move src to the incremented name;
        /// otherwise use LazyString(p_whatIfDstExists).ToString() as alternative
        /// destination name (must be file name, not folder). This way p_whatIfDstExists
        /// may be Funcᐸstringᐳ</para>
        /// p_errorLevel sets how to log the error if occurs.
        /// </summary>
        public static bool MoveFile(string p_src, string p_dest, object p_whatIfDstExists = null,
            TraceLevel p_errorLevel = TraceLevel.Error)
        {
            string msg = null;
            try
            {
                if (Directory.Exists(p_dest))
                    p_dest = Path.Combine(p_dest, Path.GetFileName(p_src));
                if (File.Exists(p_dest))
                {
                    bool? b = p_whatIfDstExists as bool?;
                    if (p_whatIfDstExists == null || b == false)
                    {
                        Utils.Logger.Info("Deleting " + p_dest);
                        File.Delete(p_dest);
                    }
                    else if (b == true)
                        p_dest = AddNumberToFileName(p_dest, 0);
                    else
                        p_dest = ((p_whatIfDstExists as LazyString) ?? new LazyString(p_whatIfDstExists)).ToString();
                }
                if (!Path.IsPathRooted(p_src) || !Path.IsPathRooted(p_dest))
                    msg = String.Format("{0} -> {1}{3}(current directory: {2})",
                        p_src, p_dest, Environment.CurrentDirectory, Environment.NewLine);
                else
                {
                    string s = Path.GetDirectoryName(p_src), d = Path.GetDirectoryName(p_dest);
                    int j = -1, n = Math.Min(s.Length, d.Length);
                    char[] separators = Path.GetInvalidFileNameChars();
                    for (int i = 0; i < n && p_src[i] == p_dest[i]; ++i)
                        j = (0 <= Array.IndexOf(separators, p_src[i])) ? i : j;
                    msg = String.Format("{0} -> {1} (in {2})",
                        p_src.Substring(j + 1), p_dest.Substring(j + 1), p_src.Substring(0, j + 1));
                }
                msg = String.Format(Path.IsPathRooted(p_src) && Path.IsPathRooted(p_dest) ? "{0} -> {1}" : "{0} -> {1}{3}(current directory: {2})",
                    p_src, p_dest, Environment.CurrentDirectory, Environment.NewLine);
                Utils.Logger.Info("Moving " + msg);
                File.Move(p_src, p_dest);
                return true;
            }
            catch (Exception e)
            {
                if (p_errorLevel != TraceLevel.Off && Utils.Logger.Level <= p_errorLevel)
                    Trace.WriteLine(Utils.Logger.FormatMessage(msg != null ? Logger.FormatExceptionMessage(e, false)
                        : Logger.FormatExceptionMessage(e, false, "while moving {0} -> {1}{3}(current directory: {2})",
                        p_src, p_dest, Environment.CurrentDirectory, Environment.NewLine), (object[])null));
                return false;
            }
        }

        /// <summary> If p_incrementIfExists=false, existing destination will be overwritten. </summary>
        public static void CopyFile(string p_src, string p_dest, bool p_incrementIfExists)
        {
            if (Directory.Exists(p_dest))
                p_dest = Path.Combine(p_dest, Path.GetFileName(p_src));
            if (p_incrementIfExists)
                p_dest = AddNumberToFileName(p_dest, 0);                // ???
            else if (File.Exists(p_dest))
                File.Delete(p_dest);
            File.Copy(p_src, p_dest);
        }

        /// <summary> Returns true if successful, false otherwise.
        /// p_pathOrPaths may be string or FileInfo, or sequence of these. </summary>
        public static bool DeleteFiles(object p_pathOrPaths)
        {
            System.Collections.IEnumerable seq;
            var fi = p_pathOrPaths as FileInfo;
            if (fi == null && p_pathOrPaths is String)
                fi = new FileInfo((string)p_pathOrPaths);
            else if (fi == null)
                return CanBe(p_pathOrPaths, out seq) 
                    && seq.Cast<object>().Count(obj => !DeleteFiles(obj)) == 0;
            try
            {
                if (fi.Exists)
                {
                    Utils.Logger.Info("Deleting " + fi.ToString());
                    fi.Delete();
                }
                return true;
            }
            catch (Exception e)
            {
                Utils.Logger.Info(Logger.FormatExceptionMessage(e, false));
                return false;
            }
        }

        /// <summary> Creates a subdirectory named "tmp9999.$$$" under p_basePath.
        /// If p_basePath==null, Path.GetTempPath() is used. </summary>
        public static DirectoryGuard CreateTmpDir(string p_basePath)
        {
            if (p_basePath == null)
                p_basePath = Path.GetTempPath();
            int startValue = (int)(Stopwatch.GetTimestamp() % 9973);
            while(true)
            {
                string name = Path.Combine(p_basePath, String.Format("tmp{0:0000}.$$$", startValue++));
                try
                {
                    if (!(new DirectoryInfo(name).Exists))
                    {
                        DirectoryInfo result = Directory.CreateDirectory(name);
                        return new DirectoryGuard(result);
                    }
                }
                catch (IOException) { }   // already exists
            };
        }

        public static void EnsureDir(string p_path, Action<string> p_messageBeforeCreation)
        {
            if (!String.IsNullOrEmpty(p_path) && !Directory.Exists(p_path))
            {
                if (p_messageBeforeCreation != null)
                    p_messageBeforeCreation(p_path);
                Directory.CreateDirectory(p_path);
            }
        }

        public static void EnsureDir(string p_path)
        {
            EnsureDir(p_path, dir => { Utils.Logger.Info("Creating directory " + dir); });
        }

        /// <summary>
        /// Returns the path where p_relPath was found or null if not found.
        /// It is searched at some fixed locations.
        /// </summary>
        public static string FindMediaFile(string p_relPath)
        {
            if (p_relPath == null)
                return p_relPath;

            // Look for p_relPath in the exe directory
            string dir = GetExeDir();
            string f = Path.Combine(dir, p_relPath);
            if (File.Exists(f))
                return f;

            // ... in the current directory
            f = Path.Combine(Environment.CurrentDirectory, p_relPath);
            if (File.Exists(f))
                return f;

            // ... all directories starting from the exe directory to top
            f = GoUpAndFind(dir, p_relPath);
            if (f != null)
                return f;

            // ... under 3rdParty/, which may be in any directory
            // starting from the exe directory to top
            f = GoUpAndFind(dir, Path.Combine("3rdParty", p_relPath));
            return f;
        }

        /// <summary>
        /// Returns the path where p_relPath was found or null if not found.
        /// The search starts at p_startDir and goes upwards only.
        /// </summary>
        public static string GoUpAndFind(string p_startDir, string p_relPath)
        {
            if (p_startDir == null)
                p_startDir = Environment.CurrentDirectory;
            while (true)
            {
                string f = Path.Combine(p_startDir, p_relPath);
                if (File.Exists(f) || Directory.Exists(f))
                    return f;
                DirectoryInfo parent = new DirectoryInfo(p_startDir).Parent;
                if (parent == null)
                    return null;
                p_startDir = parent.FullName;
            }
        }

        /// <summary> Returns filenames relative to p_dir.
        /// Follows symbolic links. Includes hidden files and folders.
        /// Omits filenames not matched by p_fnMask.
        /// </summary>
        /// <remarks>Preserves the order of DirectoryInfo.GetDirectories()
        /// and FileInfo.GetFiles(). Enumerates all subdirectories of a
        /// given directory before changing to its sibling. </remarks>
        public static IEnumerable<string> GetAllFiles(string p_dir, string p_fnMask)
        {
            if (String.IsNullOrEmpty(p_fnMask))
                p_fnMask = "*";

            var dirs = new List<string> { p_dir };
            DirectoryInfo d;
            for (int n = dirs.Count; --n >= 0; n = dirs.Count)
            {
                string dir = dirs[n];
                dirs.RemoveAt(n);
                if (String.IsNullOrEmpty(dir))
                {
                    dir = String.Empty;
                    d = new DirectoryInfo(".");
                }
                else
                    d = new DirectoryInfo(dir);
                foreach (FileInfo fn in d.GetFiles(p_fnMask))
                    yield return Path.Combine(dir, fn.Name);

                DirectoryInfo[] subdirs = d.GetDirectories();
                for (int i = subdirs.Length - 1; i >= 0; --i)
                    dirs.Add(Path.Combine(dir, subdirs[i].Name));
            }
        }

        /// <summary> Compresses p_srcDir\* to p_archivePath using 7-Zip.
        /// Contents of p_srcDir becomes the root of the created archive.
        /// The filename extension of p_archivePath specifies the archive format
        /// (.7z/.tar/.zip/.odt/.ods/.bz2).
        /// If p_deleteDir==true and the compression is successful, the directory 
        /// will be deleted recursively.
        /// Returns true if the compression was successful.
        /// </summary>
        /// <param name="p_options">Additional command line options for 7-Zip. May be null.
        /// Example: "-x!*.png" to exclude .png files,
        /// "-up0q0r2x2y2z2w2" to discard (overwrite) existing archive. </param>
        /// <exception cref="NotSupportedException">If the extension of p_archivePath is 
        /// not one of .7z, .tar, .zip (.odt .ods), .bz2 </exception>
        public static bool CompressDirectory(string p_srcDir, string p_archivePath, bool p_deleteDir, 
            string p_options, ProcessPriorityClass? p_priority = null)
        {
            string ext = Path.GetExtension(p_archivePath).ToLower();
            StringBuilder args = new StringBuilder("a -r ");
            if (ext == ".7z") // RAM usage: d48m = 48M*9.6 + 62M = ~523M (private working set) (theoretical: *11.5 instead of *9.6)
                args.Append("-t7z -m0=LZMA:d48m -mx9 -ms=on -mf=off -mmt=off");
            else if (Is(ext).EqualTo(".zip").Or(".odt").Or(".ods"))
                args.Append("-tzip -mx=9 -mpass=4 -mfb=128");
            else if (ext == ".tar")
                args.Append("-ttar");
            else if (ext == ".bz" || ext == ".bz2")
                args.Append("-tbzip2 -mx=9 -mpass=2 -mmt=on");
            else
                throw new NotSupportedException();
            if (!String.IsNullOrEmpty(p_options))
                args.Append(" " + p_options);
            string destFn = Path.GetFullPath(p_archivePath);
            args.Append(" \"");
            args.Append(destFn);
            args.Append("\" *");
            if (!Utils.Run7Zip(args.ToString(), p_srcDir, p_priority) || !File.Exists(destFn))
                return false;
            if (p_deleteDir)
                Directory.Delete(p_srcDir, true);
            return true;
        }

        /// <summary> Compresses files from p_srcFolder to p_dstFolder and/or deletes files in p_srcFolder.
        /// p_dstFolder == null means p_srcFolder. <para>
        /// p_toCompress: either ".log.txt.csv" to compress files (and subfolders) with the given extensions
        ///   from p_srcFolder to p_archName; or an IEnumerable≺string≻ of file(or folder)names in p_srcFolder
        ///   to be compressed. </para><para>
        /// p_archName: e.g. "tmp_{0:yyyy'_'MM'_'dd}.7z" where {0} is DateTime.Today. The resulting string
        ///   is appended to p_dstFolder. If the archive already exists, it gets synchronized. </para><para>
        /// p_arch2Name: e.g. "tmp_html_{0:dd}.7z"
        ///   If specified, causes files not matched by p_toCompress to be compressed to this file
        ///   (even if p_archName is null). </para>
        /// p_del:
        ///    "archivedOnly" = delete those files only that has been archived to p_arch[2]Name;
        ///    "folder" = delete the whole p_srcFolder recursively;
        ///    "all" = empty p_srcFolder, but keep it (subdirs are deleted);
        ///    "allBut.log.csv..." = delete all files/subfolders from p_srcFolder except those
        ///            having one of the given extensions;
        ///    any other string = skip deletion.
        /// </summary>
        public static void CleanUpFolder(string p_srcFolder, string p_dstFolder, object p_toCompress, string p_archName,
            string p_arch2Name = null, string p_del = "archivedOnly")
        {
            Func<string, string, bool> hasExtension = (ext, extList) => {
                if (String.IsNullOrEmpty(extList) || extList == "*") return true;
                if (String.IsNullOrEmpty(ext)) return extList.Contains("//");
                for (int a = 0; true; ) {
                    int b = extList.IndexOf(ext, a, StringComparison.OrdinalIgnoreCase);
                    if (b < 0) return false; else a = b + ext.Length;
                    if (extList.Length <= a || extList[a] == '.') return true;
                }
            };
            string[] dst = new string[] { p_archName, p_arch2Name };
            IList<string>[] lists = dst.Select(s => String.IsNullOrEmpty(s) ? (IList<string>)null : new List<string>()).ToArray();
            bool[] hasSubdir = new bool[dst.Length];
            bool useList = (p_toCompress is IEnumerable<string>);
            if (useList)
                hasSubdir[0] = (null != (lists[0] = ((IEnumerable<string>)p_toCompress).AsIList()));
            var d = new DirectoryInfo(p_srcFolder); p_srcFolder = d.FullName;
            if (!useList || lists[1] != null)
            {
                string exts = useList ? null : p_toCompress.ToStringOrNull();
                var pathEq = useList ? new Utils.EqCmp<string>(Utils.PathEquals) : null;
                foreach (FileSystemInfo fi in d.EnumerateFileSystemInfos())
                {
                    int i = (useList ? lists[0].Contains(fi.Name, pathEq) : hasExtension(fi.Extension, exts)) ? 0 : 1;
                    if (lists[i] == null || (i == 0 && useList)) continue;
                    lists[i].Add(fi.Name); hasSubdir[i] = hasSubdir[i] || (fi is DirectoryInfo);
                }
            }
            string listfn = "cleanUp-list.tmp", listf = Path.Combine(p_srcFolder, listfn);
            for (int i = 0; i < lists.Length; ++i)
                if (lists[i] != null && 0 < lists[i].Count && !String.IsNullOrEmpty(dst[i]))
                {
                    dst[i] = Path.Combine(p_dstFolder ?? p_srcFolder, Utils.FormatInvCult(dst[i], DateTime.Today));
                    File.WriteAllLines(listf, lists[i].Select(s => "\"" + s + "\""));
                    try
                    {
                        // Observation: 7612 x ~70k (512M) QuotesHistory_*.html files -> LZMA2: 8M 161sec, PPMD: 5.5M 32sec  
                        // These figures are different for *.log files.
                        bool useLZMA = hasSubdir[i] || lists[i].Any(s => !hasExtension(Path.GetExtension(s), ".htm.html.aspx"));
                        Utils.Run7Zip(String.Format("a -r -up0q0r2x2y2z1w2 -w. -ssw -t7z -mx9 {2} \"{0}\" @{1}", dst[i], listfn,
                            useLZMA ? "-m0=LZMA2:d48m -ms=on -mf=off -mmt=off" : "-m0=PPMD:mem=64m:o=32 -ms=64m"),
                            p_srcFolder, System.Diagnostics.ProcessPriorityClass.BelowNormal);
                        if (p_del == "archivedOnly")
                            foreach (string s in lists[i])
                            {
                                string fn = Path.Combine(p_srcFolder, s);
                                if (hasSubdir[i] && Directory.Exists(fn))
                                    Directory.Delete(fn, true);
                                else
                                    File.Delete(fn);
                            }
                    }
                    finally
                    {
                        Utils.TryOrLog(listf, File.Delete);
                    }
                }
            if (p_del == "folder")
                d.Delete(recursive: true);
            else if (p_del == "all" || p_del.StartsWith("allBut."))
            {
                p_del = p_del.Substr("allBut".Length);
                foreach (FileSystemInfo fi in d.EnumerateFileSystemInfos())
                {
                    if (!String.IsNullOrEmpty(p_del) && hasExtension(fi.Extension, p_del))
                        continue;
                    if (fi is DirectoryInfo)
                        ((DirectoryInfo)fi).Delete(recursive: true);
                    else
                        fi.Delete();
                }
            }
        }

        private static string g_7zPath;
        public static string SevenZipPath
        {
            get
            {
                bool debugSearch = false;
                while (g_7zPath == null)
                {
                    g_7zPath = System.Configuration.ConfigurationManager.AppSettings["7-Zip"];
                    if (!String.IsNullOrEmpty(g_7zPath) && !(debugSearch = g_7zPath.StartsWith("!!!DEBUG")))
                    {
                        g_7zPath = Path.Combine(Utils.GetExeDir(), g_7zPath);
                        break;
                    }
                    g_7zPath = FindMediaFile("7za.exe");
                    if (g_7zPath != null)
                        break;
                    g_7zPath = FindMediaFile("7z.exe");
                    if (g_7zPath != null)
                        break;
                    if (debugSearch)
                        Logger.Verbose("FindMediaFile('7z[a].exe')==null pwd=" + Environment.CurrentDirectory);
                    try
                    {
                        foreach (string keyName in new[] { @"HKEY_CURRENT_USER\Software\7-Zip", @"HKEY_LOCAL_MACHINE\SOFTWARE\7-Zip" })
                        {
                            object val = Microsoft.Win32.Registry.GetValue(keyName, "Path", null) ??
                                         Microsoft.Win32.Registry.GetValue(keyName, "Path32", null);
                            if (val != null)
                            {
                                string result = Path.Combine(val.ToString(), "7z.exe");
                                if (File.Exists(result)
                                    || File.Exists(result = Path.Combine(val.ToString(), "7za.exe")))
                                {
                                    g_7zPath = result;
                                    return g_7zPath;
                                }
                                else if (debugSearch)
                                    Logger.Verbose("Registry.GetValue({0}\\Path[32]) == {1}, 7z[a].exe not found", keyName, val);
                            }
                            else if (debugSearch)
                                Logger.Verbose("Registry.GetValue({0}\\Path[32]) == null", keyName);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.Info(Logger.FormatExceptionMessage(e, false, "occurred in {0}",
                            GetCurrentMethodName()));
                    }
                    g_7zPath = FindInPATH("7za.exe");
                    if (g_7zPath == null)
                        g_7zPath = FindInPATH("7z");
                    if (g_7zPath == null)
                    {
                        g_7zPath = String.Empty;    // don't try again
                        if (debugSearch) Logger.Verbose("FindInPATH('7z[a].exe')==null  PATH=" + Environment.GetEnvironmentVariable("PATH"));
                    }
                }
                return g_7zPath;
            }
            set
            {
                g_7zPath = value;
            }
        }

        private static string g_rarPath;
        public static string RarPath
        {
            get
            {
                while (g_rarPath == null)
                {
                    g_rarPath = System.Configuration.ConfigurationManager.AppSettings["Rar"];
                    if (!String.IsNullOrEmpty(g_rarPath))
                    {
                        g_rarPath = Path.Combine(Utils.GetExeDir(), g_rarPath);
                        break;
                    }
                    if (null != (g_rarPath = FindMediaFile("Rar.exe")))
                        break;
                    try
                    {
                        // WinRar occurrences in the registry:
                        // HKEY_CLASSES_ROOT\Applications\WinRAR.exe\shell\open\command
                        //      "C:\Program Files\WinRAR\WinRAR.exe" "%1"
                        // HKEY_CLASSES_ROOT\CLSID\{B41DB860-64E4-11D2-9906-E49FADC173CA}\InProcServer32
                        //      C:\Program Files\WinRAR\rarext.dll       this is the GUID of the shell context menu handler
                        // HKEY_CLASSES_ROOT\WinRAR\DefaultIcon
                        //      C:\Program Files\WinRAR\WinRAR.exe,0
                        // HKEY_LOCAL_MACHINE\SOFTWARE\Classes\CLSID\{B41DB860-64E4-11D2-9906-E49FADC173CA}\InProcServer32
                        // HKEY_LOCAL_MACHINE\SOFTWARE\Classes\WinRAR\DefaultIcon
                        // HKEY_CURRENT_USER\Software\WinRAR SFX
                        //      C%%Program Files (x86)%WinRAR = C:\Program Files (x86)\WinRAR
                        //      C%%Program Files%WinRAR = C:\Program Files\WinRAR
                        // HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\WinRAR.exe
                        //      Path = C:\Program Files\WinRAR
                        //      gyanitom h ez csak akkor van ha mar legalabb 1x elinditotta a user
                        // HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\WinRAR archiver
                        //      UninstallString = C:\Program Files\WinRAR\uninstall.exe
                        // HKEY_LOCAL_MACHINE\SOFTWARE\WinRAR   -- this does not exist for old versions (e.g. 3.51 x86)
                        //      exe64 = C:\Program Files\WinRAR\WinRAR.exe

                        var key = Microsoft.Win32.Registry.LocalMachine.OpenSubKey(@"SOFTWARE\Classes\WinRAR\DefaultIcon");
                        object val;
                        if (key != null && null != (val = key.GetValue(null)))
                        {
                            string result = Path.Combine(Utils.GetParent(val.ToString()), "Rar.exe");
                            if (File.Exists(result))
                            {
                                g_rarPath = result;
                                break;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.Info(Logger.FormatExceptionMessage(e, false, "occurred in {0}",
                            GetCurrentMethodName()));
                    }
                    if (g_rarPath == null)
                        g_rarPath = String.Empty;    // don't try again
                }
                return g_rarPath;
            }
            set
            {
                g_rarPath = value;
            }
        }

        /// <summary> Returns true if 7z.exe was executed successfully.
        /// In case of error, the stdout of 7z.exe is logged (because
        /// 7-Zip uses stdout for error messages) </summary>
        public static bool Run7Zip(string p_args, string p_startDir, ProcessPriorityClass? p_priority)
        {
            StringBuilder stdOut = null;
            string _7z = SevenZipPath;
            if (String.IsNullOrEmpty(_7z) || !File.Exists(_7z))
            {
                Utils.Logger.Error("Cannot find 7z.exe or 7za.exe (SevenZipPath=\"{0}\")", _7z);
                return false;
            }
            int? exitCode = RunCmdLineTool(_7z, p_args, p_startDir,
                p_priority, p_stdOut: (sender, arg) => {
                    if (stdOut == null)
                        Utils.Create(out stdOut);
                    stdOut.AppendLine(arg.Data);
                });
            if (exitCode == 0)
                return true;
            if (exitCode.HasValue || stdOut != null)
                Utils.Logger.Warning("*** Warning: {0} returned exit code={1}{2}",
                    Path.GetFileName(_7z), exitCode, stdOut == null ? null 
                    : ", output:\n" + stdOut.ToString());
            return false;
        }

        /// <summary> Returns true if Rar.exe was executed successfully. </summary>
        public static bool RunRar(string p_args, string p_startDir, ProcessPriorityClass? p_priority)
        {
            StringBuilder stdErr = null;
            string rar = RarPath;
            if (String.IsNullOrEmpty(rar) || !File.Exists(rar))
            {
                Utils.Logger.Error("Cannot find Rar.exe");
                return false;
            }
            int? exitCode = RunCmdLineTool(rar, p_args, p_startDir,
                p_priority, p_stdErr: (sender, arg) => {
                    if (stdErr == null)
                        Utils.Create(out stdErr);
                    stdErr.AppendLine(arg.Data);
                });
            if (exitCode == 0)
                return true;
            if (exitCode.HasValue || stdErr != null)
                Utils.Logger.Warning("*** Warning: {0} returned exit code={1}{2}",
                    Path.GetFileName(rar), exitCode, stdErr == null ? null 
                    : ", stderr:\n" + stdErr.ToString());
            return false;
        }
    }

    /// <summary> Helper class to delete a directory recursively 
    /// when this object is disposed or destroyed. </summary>
    public class DirectoryGuard : DisposablePattern
    {
        public DirectoryInfo Dir { get; private set; }
        public DirectoryGuard(DirectoryInfo p_dir) { Dir = p_dir; }
        public override string ToString() { return Dir.FullName; }
        protected override void Dispose(bool p_notFromFinalize)
        {
            if (Dir != null)
            {
                DirectoryInfo tmp = Dir;
                Dir = null;
                tmp.Delete(true);
            }
        }
    }


}