package genepi.imputationserver.steps;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.collections4.ListUtils;

import cloudgene.sdk.internal.IExternalWorkspace;
import cloudgene.sdk.internal.WorkflowContext;
import cloudgene.sdk.internal.WorkflowStep;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.imputation.ImputationPipeline;
import genepi.imputationserver.steps.vcf.MergedVcfFile;
import genepi.imputationserver.util.DefaultPreferenceStore;
import genepi.imputationserver.util.FileChecksum;
import genepi.imputationserver.util.FileMerger;
import genepi.imputationserver.util.ImputationResults;
import genepi.imputationserver.util.ImputedChromosome;
import genepi.imputationserver.util.PasswordCreator;
import genepi.imputationserver.util.PgsPanel;
import genepi.io.FileUtil;
import genepi.io.text.LineWriter;
import genepi.riskscore.io.MetaFile;
import genepi.riskscore.io.OutputFile;
import genepi.riskscore.io.ReportFile;
import genepi.riskscore.tasks.CreateHtmlReportTask;
import genepi.riskscore.tasks.MergeReportTask;
import genepi.riskscore.tasks.MergeScoreTask;
import lukfor.progress.TaskService;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.AesKeyStrength;
import net.lingala.zip4j.model.enums.CompressionLevel;
import net.lingala.zip4j.model.enums.CompressionMethod;
import net.lingala.zip4j.model.enums.EncryptionMethod;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class CompressionEncryption extends WorkflowStep {

    private class BatchRunner implements Callable<String>{
	private WorkflowContext context;
	private LineWriter writer;
	private String tempdir;
	private String password;
	private List<String> chroms;
	
	public BatchRunner(WorkflowContext context,LineWriter writer,String tempdir,String password,List<String> chroms){
	    this.context=context;
	    this.writer=writer;
	    this.password=password;
	    this.tempdir=tempdir;
	    this.chroms=chroms;
	}
	
	@Override
	public String call() throws Exception {
	    List<String> res=new ArrayList<String>();
	    for (String c:chroms){
		zipEncryptChr(context,c,writer,password,tempdir);
		res.add(c);
	    }
	    return "Chromosome(s) "+String.join(", ",res)+" finished";
	}
    }
    
    @Override
    public boolean run(WorkflowContext context) {
      	int nthreads=1; // default
	
	String output = context.get("outputimputation");
	String localOutput = context.get("local");
	String mode = context.get("mode");

	boolean phasingOnly = false;
	if (mode != null && mode.equals("phasing"))
	    phasingOnly = true;

	String password = context.get("password");
	if (password == null || (password != null && password.equals("auto")))
	    password = PasswordCreator.createPassword();
	
	File jobConfig = new File(FileUtil.path(getFolder(CompressionEncryption.class),"job.config"));
	DefaultPreferenceStore store = new DefaultPreferenceStore();
	if (jobConfig.exists())
	    store.load(jobConfig);
	else
	    context.log("Configuration file '" + jobConfig.getAbsolutePath() + "' not available. Using default values.");
	if (store.getString("export.threads") != null && !store.getString("export.threads").equals("")){
	    try{
		nthreads=Integer.parseInt(store.getString("export.threads"));
	    }
	    catch (NumberFormatException e){
		e.printStackTrace();
	    }
	}

	if (nthreads==1)
	    context.log("Export: using 1 thread");
	else
	    context.log("Export: using up to "+nthreads+" threads");

	try {
	    //--------------------------
	    context.beginTask("Export data ...");
	    List<String> folders = HdfsUtil.getDirectories(output);
	    ImputationResults imputationResults = new ImputationResults(folders, phasingOnly);
	    Map<String, ImputedChromosome> imputedChromosomes = imputationResults.getChromosomes();
	    List<String> all_chr=new ArrayList<>(imputationResults.getChromosomes().keySet());
	    int n=all_chr.size()/nthreads;
	    if (all_chr.size()%nthreads!=0)
		n++;
	    List<List<String>> chr_batches=ListUtils.partition(all_chr,n);
	    context.log(chr_batches.size()+" batch(es):");
	    for (List<String> x:chr_batches)
		context.log(String.join(", ",x));
	    LineWriter writer = new LineWriter(FileUtil.path(localOutput,"results.md5"));
	    String temp = FileUtil.path(localOutput,"temp");
	    FileUtil.createDirectory(temp);
	    
	    //--------------------------
	    
	    // List<Thread> threads=new ArrayList<Thread>();
	    // final String pwd=password;
	    // for (int i=0;i<chr_batches.size();i++){
	    // 	final List<String> L=chr_batches.get(i);
	    // 	Thread x=new Thread(){
	    // 		@Override
	    // 		public void run(){
	    // 		    for (String c:L){
	    // 			try{
	    // 			    zipEncryptChr(context,c,writer,pwd,temp);
	    // 			}catch(Exception e){
	    // 			    //throw e;
	    // 			}
	    // 		    }
	    // 		}};
	    // 	threads.add(x);
	    // }
	    // for(Thread t:threads)
	    // 	t.start();
	    // for(Thread t:threads)
	    // 	t.join();

	    //--------------------------

	    ExecutorService pool=Executors.newFixedThreadPool(chr_batches.size());
	    List<Callable<String>> callables=new ArrayList<Callable<String>> ();
	    for (int i=0;i<chr_batches.size();i++)
		callables.add(new BatchRunner(context,writer,temp,password,chr_batches.get(i)));
	    List<Future<String>> res=pool.invokeAll(callables);
	    for (Future<String> r:res)
		context.log(r.get());
	    pool.shutdown();
	    if (!pool.awaitTermination(60L,TimeUnit.SECONDS))
		context.log("Thread pool did not terminate");
	    
	    //--------------------------
	    
	    FileUtil.deleteDirectory(temp);
	    writer.close();
	    HdfsUtil.delete(output);
	    context.endTask("Exported data", WorkflowContext.OK);
	} catch (Exception e) {
	    e.printStackTrace();
	    context.endTask("Data export failed: " + e.getMessage(), WorkflowContext.ERROR);
	    return false;
	}

	// submit counters!
	context.submitCounter("samples");
	context.submitCounter("genotypes");
	context.submitCounter("chromosomes");
	context.submitCounter("runs");
	// submit panel and phasing method counters
	String reference = context.get("refpanel");
	String phasing = context.get("phasing");
	context.submitCounter("refpanel_" + reference);
	context.submitCounter("phasing_" + phasing);
	context.submitCounter("23andme-input");

	String notification = "no";
	if (store.getString("minimac.sendmail") != null && !store.getString("minimac.sendmail").equals(""))
	    notification = store.getString("minimac.sendmail");
	String serverUrl = "https://imputationserver.sph.umich.edu";
	if (store.getString("server.url") != null && !store.getString("server.url").isEmpty())
	    serverUrl = store.getString("server.url");
	
	// send email
	if (notification.equals("yes")) {
	    Object mail = context.getData("cloudgene.user.mail");
	    Object name = context.getData("cloudgene.user.name");

	    if (mail != null) {

		String subject = "Job " + context.getJobId() + " is complete.";
		String message = "Dear " + name + ",\nthe password for the imputation results is: " + password
		    + "\n\nThe results can be downloaded from " + serverUrl + "/start.html#!jobs/"
		    + context.getJobId() + "/results";

		try {
		    context.sendMail(subject, message);
		    context.ok("We have sent an email to <b>" + mail + "</b> with the password.");
		    return true;
		} catch (Exception e) {
		    context.error("Data compression failed: " + e.getMessage());
		    return false;
		}

	    } else {
		context.error("No email address found. Please enter your email address (Account -> Profile).");
		return false;
	    }

	} else {
	    context.ok("Email notification is disabled. All results are encrypted with password <b>" + password + "</b>");
	    return true;
	}
    }

    public void createEncryptedZipFile(File file, List<File> files, String password, boolean aesEncryption)
	throws ZipException {
	ZipParameters param = new ZipParameters();
	param.setEncryptFiles(true);
	param.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);

	if (aesEncryption) {
	    param.setEncryptionMethod(EncryptionMethod.AES);
	    param.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
	    param.setCompressionMethod(CompressionMethod.DEFLATE);
	    param.setCompressionLevel(CompressionLevel.NORMAL);
	}

	ZipFile zipFile = new ZipFile(file, password.toCharArray());
	zipFile.addFiles(files, param);

    }

    public void createEncryptedZipFile(File file, File source, String password, boolean aesEncryption)
	throws ZipException {
	List<File> files = new Vector<File>();
	files.add(source);
	createEncryptedZipFile(file, files, password, aesEncryption);
    }

    private void zipEncryptChr(WorkflowContext context,String cname,LineWriter writer,String password,String tempdir) throws Exception{
	String output = context.get("outputimputation");
	String localOutput = context.get("local");
	String aesEncryptionValue = context.get("aesEncryption");
	String meta = context.get("meta");
	String mode = context.get("mode");

	boolean phasingOnly = false;
	if (mode != null && mode.equals("phasing")) {
	    phasingOnly = true;
	}

	boolean mergeMetaFiles = !phasingOnly && (meta != null && meta.equals("yes"));
	boolean aesEncryption = (aesEncryptionValue != null && aesEncryptionValue.equals("yes"));
	
	ImputationResults imputationResults = new ImputationResults(HdfsUtil.getDirectories(output),phasingOnly);
	ImputedChromosome imputedChromosome=imputationResults.getChromosomes().get(cname);
	synchronized (this) {context.log("Export and merge chromosome " + cname);}

	// output files
	ArrayList<File> files = new ArrayList<File>();

	// merge info files
	if (!phasingOnly) {
	    String infoOutput = FileUtil.path(tempdir, "chr" + cname + ".info.gz");
	    FileMerger.mergeAndGzInfo(imputedChromosome.getInfoFiles(), infoOutput);
	    files.add(new File(infoOutput));
	}

	// merge all dosage files
	String dosageOutput;
	if (phasingOnly) {
	    dosageOutput = FileUtil.path(tempdir, "chr" + cname + ".phased.vcf.gz");
	} else {
	    dosageOutput = FileUtil.path(tempdir, "chr" + cname + ".dose.vcf.gz");
	}
	files.add(new File(dosageOutput));

	MergedVcfFile vcfFile = new MergedVcfFile(dosageOutput);
	vcfFile.addHeader(context, imputedChromosome.getHeaderFiles());
	for (String file : imputedChromosome.getDataFiles()) {
	    synchronized (this) {context.log("Read file " + file);}
	    vcfFile.addFile(HdfsUtil.open(file));
	    HdfsUtil.delete(file);
	}
	vcfFile.close();

	// merge all meta files
	if (mergeMetaFiles) {
	    synchronized (this) {context.log("Merging meta files...");}
	    String dosageMetaOutput = FileUtil.path(tempdir, "chr" + cname + ".empiricalDose.vcf.gz");
	    MergedVcfFile vcfFileMeta = new MergedVcfFile(dosageMetaOutput);
	    String headerMetaFile = imputedChromosome.getHeaderMetaFiles().get(0);
	    synchronized (this) {context.log("Use header from file " + headerMetaFile);}
	    vcfFileMeta.addFile(HdfsUtil.open(headerMetaFile));

	    for (String file : imputedChromosome.getDataMetaFiles()) {
		synchronized (this) {context.log("Read file " + file);}
		vcfFileMeta.addFile(HdfsUtil.open(file));
		HdfsUtil.delete(file);
	    }
	    vcfFileMeta.close();
	    synchronized (this) {context.log("Meta files merged");}
	    files.add(new File(dosageMetaOutput));
	}

	// create zip file
	String fileName = "chr_" + cname + ".zip";
	String filePath = FileUtil.path(localOutput,fileName);
	File file = new File(filePath);
	createEncryptedZipFile(file,files,password,aesEncryption);

	// add checksum to hash file
	synchronized (this) {context.log("Creating file checksum for " + filePath);}
	long checksumStart = System.currentTimeMillis();
	String checksum = FileChecksum.HashFile(new File(filePath), FileChecksum.Algorithm.MD5);
	synchronized (this) {writer.write(checksum + " " + fileName);}
	long checksumEnd = (System.currentTimeMillis() - checksumStart) / 1000;
	synchronized (this) {context.log("File checksum for " + filePath + " created in " + checksumEnd + " seconds.");}

	IExternalWorkspace externalWorkspace = context.getExternalWorkspace();
	if (externalWorkspace != null) {
	    long start = System.currentTimeMillis();
	    synchronized (this) {context.log("External Workspace '" + externalWorkspace.getName() + "' found");}
	    synchronized (this) {context.log("Start file upload: " + filePath);}
	    String url = externalWorkspace.upload("local", file);
	    long end = (System.currentTimeMillis() - start) / 1000;
	    synchronized (this) {context.log("Upload finished in  " + end + " sec. File Location: " + url);}
	    synchronized (this) {context.log("Add " + localOutput + " to custom download");}
	    String size = FileUtils.byteCountToDisplaySize(file.length());
	    context.addDownload("local", fileName, size, url);
	    FileUtil.deleteFile(filePath);
	    synchronized (this) {context.log("File deleted: " + filePath);}
	} else {
	    synchronized (this) {context.log("No external Workspace set.");}
	}	    
    }

    //----------------------------------------------------------------------------

    // @Override
    // public boolean run(WorkflowContext context) {

    // 	String workingDirectory = getFolder(CompressionEncryption.class);

    // 	String output = context.get("outputimputation");
    // 	String outputScores = context.get("outputScores");
    // 	String localOutput = context.get("local");
    // 	String aesEncryptionValue = context.get("aesEncryption");
    // 	String meta = context.get("meta");
    // 	String mode = context.get("mode");
    // 	String password = context.get("password");

    // 	PgsPanel pgsPanel = PgsPanel.loadFromProperties(context.getData("pgsPanel"));

    // 	boolean phasingOnly = false;
    // 	if (mode != null && mode.equals("phasing")) {
    // 	    phasingOnly = true;
    // 	}

    // 	boolean mergeMetaFiles = !phasingOnly && (meta != null && meta.equals("yes"));

    // 	boolean aesEncryption = (aesEncryptionValue != null && aesEncryptionValue.equals("yes"));

    // 	// read config if mails should be sent
    // 	String folderConfig = getFolder(CompressionEncryption.class);
    // 	File jobConfig = new File(FileUtil.path(folderConfig, "job.config"));
    // 	DefaultPreferenceStore store = new DefaultPreferenceStore();
    // 	if (jobConfig.exists()) {
    // 	    store.load(jobConfig);
    // 	} else {
    // 	    context.log("Configuration file '" + jobConfig.getAbsolutePath() + "' not available. Use default values.");
    // 	}

    // 	String notification = "no";
    // 	if (store.getString("minimac.sendmail") != null && !store.getString("minimac.sendmail").equals("")) {
    // 	    notification = store.getString("minimac.sendmail");
    // 	}

    // 	String serverUrl = "https://imputationserver.sph.umich.edu";
    // 	if (store.getString("server.url") != null && !store.getString("server.url").isEmpty()) {
    // 	    serverUrl = store.getString("server.url");
    // 	}

    // 	String sanityCheck = "yes";
    // 	if (store.getString("sanitycheck") != null && !store.getString("sanitycheck").equals("")) {
    // 	    sanityCheck = store.getString("sanitycheck");
    // 	}

    // 	if (password == null || (password != null && password.equals("auto"))) {
    // 	    password = PasswordCreator.createPassword();
    // 	}

    // 	try {

    // 	    context.beginTask("Export data...");

    // 	    // get sorted directories
    // 	    List<String> folders = HdfsUtil.getDirectories(output);

    // 	    ImputationResults imputationResults = new ImputationResults(folders, phasingOnly);
    // 	    Map<String, ImputedChromosome> imputedChromosomes = imputationResults.getChromosomes();

    // 	    Set<String> chromosomes = imputedChromosomes.keySet();
    // 	    boolean lastChromosome = false;
    // 	    int index = 0;

    // 	    String checksumFilename = FileUtil.path(localOutput, "results.md5");
    // 	    LineWriter writer = new LineWriter(checksumFilename);

    // 	    for (String name : chromosomes) {

    // 		index++;

    // 		if (index == chromosomes.size()) {
    // 		    lastChromosome = true;
    // 		}

    // 		ImputedChromosome imputedChromosome = imputedChromosomes.get(name);

    // 		context.println("Export and merge chromosome " + name);

    // 		// create temp dir
    // 		String temp = FileUtil.path(localOutput, "temp");
    // 		FileUtil.createDirectory(temp);

    // 		// output files

    // 		ArrayList<File> files = new ArrayList<File>();

    // 		// merge info files
    // 		if (!phasingOnly) {
    // 		    String infoOutput = FileUtil.path(temp, "chr" + name + ".info.gz");
    // 		    FileMerger.mergeAndGzInfo(imputedChromosome.getInfoFiles(), infoOutput);
    // 		    files.add(new File(infoOutput));
    // 		}

    // 		// merge all dosage files

    // 		String dosageOutput;
    // 		if (phasingOnly) {
    // 		    dosageOutput = FileUtil.path(temp, "chr" + name + ".phased.vcf.gz");
    // 		} else {
    // 		    dosageOutput = FileUtil.path(temp, "chr" + name + ".dose.vcf.gz");
    // 		}
    // 		files.add(new File(dosageOutput));

    // 		MergedVcfFile vcfFile = new MergedVcfFile(dosageOutput);
    // 		vcfFile.addHeader(context, imputedChromosome.getHeaderFiles());

    // 		for (String file : imputedChromosome.getDataFiles()) {
    // 		    context.println("Read file " + file);
    // 		    vcfFile.addFile(HdfsUtil.open(file));
    // 		    HdfsUtil.delete(file);
    // 		}

    // 		vcfFile.close();

    // 		// merge all meta files
    // 		if (mergeMetaFiles) {

    // 		    context.println("Merging meta files...");

    // 		    String dosageMetaOutput = FileUtil.path(temp, "chr" + name + ".empiricalDose.vcf.gz");
    // 		    MergedVcfFile vcfFileMeta = new MergedVcfFile(dosageMetaOutput);

    // 		    String headerMetaFile = imputedChromosome.getHeaderMetaFiles().get(0);
    // 		    context.println("Use header from file " + headerMetaFile);

    // 		    vcfFileMeta.addFile(HdfsUtil.open(headerMetaFile));

    // 		    for (String file : imputedChromosome.getDataMetaFiles()) {
    // 			context.println("Read file " + file);
    // 			vcfFileMeta.addFile(HdfsUtil.open(file));
    // 			HdfsUtil.delete(file);
    // 		    }
    // 		    vcfFileMeta.close();

    // 		    context.println("Meta files merged.");

    // 		    files.add(new File(dosageMetaOutput));
    // 		}

    // 		if (sanityCheck.equals("yes") && lastChromosome) {
    // 		    context.log("Run tabix on chromosome " + name + "...");
    // 		    Command tabix = new Command(FileUtil.path(workingDirectory, "bin", "tabix"));
    // 		    tabix.setSilent(false);
    // 		    tabix.setParams("-f", dosageOutput);
    // 		    if (tabix.execute() != 0) {
    // 			context.endTask("Error during index creation: " + tabix.getStdOut(), WorkflowContext.ERROR);
    // 			return false;
    // 		    }
    // 		    context.log("Tabix done.");
    // 		}

    // 		// create zip file
    // 		String fileName = "chr_" + name + ".zip";
    // 		String filePath = FileUtil.path(localOutput, fileName);
    // 		File file = new File(filePath);
    // 		createEncryptedZipFile(file, files, password, aesEncryption);

    // 		// add checksum to hash file
    // 		context.log("Creating file checksum for " + filePath);
    // 		long checksumStart = System.currentTimeMillis();
    // 		String checksum = FileChecksum.HashFile(new File(filePath), FileChecksum.Algorithm.MD5);
    // 		writer.write(checksum + " " + fileName);
    // 		long checksumEnd = (System.currentTimeMillis() - checksumStart) / 1000;
    // 		context.log("File checksum for " + filePath + " created in " + checksumEnd + " seconds.");

    // 		// delete temp dir
    // 		FileUtil.deleteDirectory(temp);

    // 		IExternalWorkspace externalWorkspace = context.getExternalWorkspace();

    // 		if (externalWorkspace != null) {

    // 		    long start = System.currentTimeMillis();

    // 		    context.log("External Workspace '" + externalWorkspace.getName() + "' found");

    // 		    context.log("Start file upload: " + filePath);

    // 		    String url = externalWorkspace.upload("local", file);

    // 		    long end = (System.currentTimeMillis() - start) / 1000;

    // 		    context.log("Upload finished in  " + end + " sec. File Location: " + url);

    // 		    context.log("Add " + localOutput + " to custom download");

    // 		    String size = FileUtils.byteCountToDisplaySize(file.length());

    // 		    context.addDownload("local", fileName, size, url);

    // 		    FileUtil.deleteFile(filePath);

    // 		    context.log("File deleted: " + filePath);

    // 		} else {
    // 		    context.log("No external Workspace set.");
    // 		}
    // 	    }

    // 	    writer.close();

    // 	    // delete temporary files
    // 	    HdfsUtil.delete(output);

    // 	    // Export calculated risk scores
    // 	    if (pgsPanel != null) {

    // 		context.println("Exporting PGS scores...");

    // 		String temp2 = FileUtil.path(localOutput, "temp2");
    // 		FileUtil.createDirectory(temp2);

    // 		List<String> scoreList = HdfsUtil.getFiles(outputScores);

    // 		String[] chunksScores = new String[scoreList.size() / 2];
    // 		String[] chunksReports = new String[scoreList.size() / 2];

    // 		int chunksScoresCount = 0;
    // 		int chunksReportsCount = 0;

    // 		for (String score : scoreList) {

    // 		    String filename = FileUtil.getFilename(score);
    // 		    String localPath = FileUtil.path(temp2, filename);
    // 		    HdfsUtil.get(score, localPath);

    // 		    if (score.endsWith(".json")) {
    // 			chunksReports[chunksReportsCount] = localPath;
    // 			chunksReportsCount++;
    // 		    } else {
    // 			chunksScores[chunksScoresCount] = localPath;
    // 			chunksScoresCount++;
    // 		    }

    // 		}

    // 		String outputFileScores = FileUtil.path(temp2, "scores.txt");
    // 		String outputFileReports = FileUtil.path(temp2, "report.json");
    // 		String outputFileHtml = FileUtil.path(localOutput, "scores.html");

    // 		// disable ansi
    // 		TaskService.setAnsiSupport(false);

    // 		MergeScoreTask mergeScore = new MergeScoreTask();
    // 		mergeScore.setInputs(chunksScores);
    // 		mergeScore.setOutput(outputFileScores);
    // 		TaskService.run(mergeScore);

    // 		MergeReportTask mergeReport = new MergeReportTask();
    // 		mergeReport.setInputs(chunksReports);
    // 		mergeReport.setOutput(outputFileReports);
    // 		TaskService.run(mergeReport);

    // 		ReportFile report = mergeReport.getResult();

    // 		String folder = getFolder(CompressionEncryption.class);

    // 		MetaFile metaFile = MetaFile.load(FileUtil.path(folder, "pgs-catalog.json"));
    // 		report.mergeWithMeta(metaFile);

    // 		CreateHtmlReportTask htmlReportTask = new CreateHtmlReportTask();
    // 		htmlReportTask.setApplicationName("");
    // 		htmlReportTask.setVersion("PGS Server Beta <small>(" +  ImputationPipeline.PIPELINE_VERSION + ")</small>");
    // 		htmlReportTask.setShowCommand(false);
    // 		htmlReportTask.setReport(report);
    // 		htmlReportTask.setOutput(outputFileHtml);
    // 		TaskService.run(htmlReportTask);

    // 		context.println("Created html report " + outputFileHtml + ".");

    // 		String fileName = "scores.zip";
    // 		String filePath = FileUtil.path(localOutput, fileName);
    // 		File file = new File(filePath);
    // 		createEncryptedZipFile(file, new File(outputFileScores), password, aesEncryption);

    // 		context.println("Exported PGS scores to " + fileName + ".");

    // 		FileUtil.deleteDirectory(temp2);
    // 	    }

    // 	    context.endTask("Exported data.", WorkflowContext.OK);

    // 	} catch (Exception e) {
    // 	    e.printStackTrace();
    // 	    context.endTask("Data export failed: " + e.getMessage(), WorkflowContext.ERROR);
    // 	    return false;
    // 	}

    // 	// submit counters!
    // 	context.submitCounter("samples");
    // 	context.submitCounter("genotypes");
    // 	context.submitCounter("chromosomes");
    // 	context.submitCounter("runs");
    // 	// submit panel and phasing method counters
    // 	String reference = context.get("refpanel");
    // 	String phasing = context.get("phasing");
    // 	context.submitCounter("refpanel_" + reference);
    // 	context.submitCounter("phasing_" + phasing);
    // 	context.submitCounter("23andme-input");

    // 	// send email
    // 	if (notification.equals("yes")) {

    // 	    Object mail = context.getData("cloudgene.user.mail");
    // 	    Object name = context.getData("cloudgene.user.name");

    // 	    if (mail != null) {

    // 		String subject = "Job " + context.getJobId() + " is complete.";
    // 		String message = "Dear " + name + ",\nthe password for the imputation results is: " + password
    // 		    + "\n\nThe results can be downloaded from " + serverUrl + "/start.html#!jobs/"
    // 		    + context.getJobId() + "/results";

    // 		try {
    // 		    context.sendMail(subject, message);
    // 		    context.ok("We have sent an email to <b>" + mail + "</b> with the password.");
    // 		    return true;
    // 		} catch (Exception e) {
    // 		    context.error("Data compression failed: " + e.getMessage());
    // 		    return false;
    // 		}

    // 	    } else {
    // 		context.error("No email address found. Please enter your email address (Account -> Profile).");
    // 		return false;
    // 	    }

    // 	} else {
    // 	    context.ok(
    // 		       "Email notification is disabled. All results are encrypted with password <b>" + password + "</b>");
    // 	    return true;
    // 	}
    // }
    
    //----------------------------------------------------------------------------


}
